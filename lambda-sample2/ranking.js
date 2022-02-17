'use strict';
const aws = require("aws-sdk");
aws.config.update({
    region: process.env.REGION
})
const {Kafka} = require('kafkajs')
const moment = require("moment");
const s3 = new aws.S3();
const AthenaExpress = require("athena-express");
var i18n = new (require('i18n-2'))({
    locales: ['en', 'ko', 'id', 'th', 'zht', 'fr'] // default locale english
});

const athenaExpressConfig = {
    aws,
    db: process.env.ATHENA_DB,
    s3: process.env.QUERY_RESULT,
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

const EXCLUDE_IDS = {
    "twn": [],
    "tha": [],
    "idn": [],
    "kor": [877],
    "fra": []
}

// Get Ranking Target Group : 리전의 Language별로 랭킹을 만들어야 함
async function getGroupInfos(targetStartDate, targetEndDate) {
    const query = `SELECT region, user_language
                   FROM 
                        "${process.env.ATHENA_DB}".stat_content_summary_hourly
                   WHERE 
                        create_dt >= '${targetStartDate}'
                   AND
                        create_dt < '${targetEndDate}'
                   GROUP BY region, user_language`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getGroupInfos', err);
    }
}

// 지난 24시간 동안 많이 본 작품, 많이 찜한작품 조회
async function getContentSummary(targetStartDate, targetEndDate, region, language, orderBy) {
    let query = `
            SELECT 
                content_id,
                sum(open_cnt) as open_cnt,
                sum(favorite_cnt) as favorite_cnt
            FROM 
                "${process.env.ATHENA_DB}".stat_content_summary_hourly
            WHERE 
                create_dt >= '${targetStartDate}'
            AND
                create_dt < '${targetEndDate}'
            AND 
                region = '${region}'
            AND 
                user_language = '${language}'`;

    if(EXCLUDE_IDS[process.env.LOCALE].length > 0)
        query += ` AND content_id not in (${EXCLUDE_IDS[process.env.LOCALE].join(',')})`;

    query += ` GROUP BY content_id
            ORDER BY ${orderBy} DESC
            LIMIT 20`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getContentSummary', err);
    }
}

// 오늘의 매칭 작품을 Kafka 도메인 이벤트로 발행한다.
async function produceKafkaMessage(message) {
    try {
        // Setup kafka js
        const kafka = new Kafka({
            clientId: process.env.KAFKA_TOPIC,
            brokers: process.env.KAFKA_BROKER.split(",")
        })
        const trimMessage = JSON.stringify(JSON.parse(message));
        console.info(`[DomainMessage] - ${trimMessage}`);
        const producer = kafka.producer()
        await producer.connect()
        await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: [
                {value: trimMessage}
            ],
        })
        await producer.disconnect();
    } catch (err) {
        console.error('[ERROR] produceKafkaMessage', err);
    }
}

async function createPartitions(table) {
    // 최근 3시간 파티션 재 생성
    for (let i = 0; i < 3; i++) {
        let dt = moment().utc().subtract(i, 'hour').format('YYYY-MM-DD-HH');
        let prefix = `${table}/create_dt=${dt}`;

        let params = {
            Bucket: process.env.S3_BUCKET_NAME,
            Prefix: prefix
        }
        const listedObjects = await s3.listObjects(params).promise();
        if (listedObjects.Contents.length < 1) {
            console.info(`S3 Object does not exist : ${prefix}`);
            continue;
        }
        const splits = prefix.split("/");
        if (splits.length > 1) {
            let partition = splits[1];
            const parsePartition = partition.split("=");
            partition = `${parsePartition[0]}='${parsePartition[1]}'`
            createPartition(partition, table);
        }
    }
}

async function createPartition(partition, table) {
    let query = `ALTER TABLE ${process.env.ATHENA_DB}.${table} ADD IF NOT EXISTS PARTITION(${partition})`;
    try {
        await athenaExpress.query(query).then(function () {
            console.info(`[SUCC] createPartition - ${query}`);
        });
    } catch (error) {
        console.error(`[ERROR] createPartition : ${query} - ${error}`);
    }
}

async function makeDomainEvent(region, language, summaryByOpenCnt, summaryByFavoriteCnt) {
    // 도메인 이벤트 메시지 구성
    let attributes = [];
    if (summaryByOpenCnt.Items) {
        console.info("[sort by open_cnt]");
        console.info(summaryByOpenCnt.Items);
        let ids = [];
        summaryByOpenCnt.Items.forEach(function (item) {
            ids.push(item.content_id);
        });

        i18n.setLocale(language);
        let title = i18n.__('contenthome_universe_matching_type_today_mostviewed');
        attributes.push(`{
                            "type": "TODAY_READ",
                            "contents":{
                                "title":"${title}",
                                "ids": [${ids.join(',')}]
                            }
                        }`);
    }
    if (summaryByFavoriteCnt.Items) {
        console.info("[sort by favorite_cnt]");
        console.info(summaryByFavoriteCnt.Items);
        let ids = [];
        summaryByFavoriteCnt.Items.forEach(function (item) {
            ids.push(item.content_id);
        });
        i18n.setLocale(language);
        let title = i18n.__('contenthome_universe_matching_type_today_mostpopular');
        attributes.push(`{
                            "type": "TODAY_SUBSCRIPTION",
                            "contents":{
                                "title":"${title}",
                                "ids": [${ids.join(',')}]
                            }
                        }`);
    }
    const message = `{
                    "event": {
                        "type": "CREATE",
                        "attributes":[{
                            "region": "${region}",
                            "language": "${language}",
                            "list": [${attributes.toString()}]
                        }]
                    },
                    "created": "${moment.utc().format()}"
                }`;
    return message;
}

module.exports = {
    getGroupInfos,
    getContentSummary,
    produceKafkaMessage,
    createPartitions,
    makeDomainEvent
}
