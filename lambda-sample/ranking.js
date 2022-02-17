'use strict';
const aws = require("aws-sdk");
aws.config.update({
    region: process.env.REGION
})
const {Kafka} = require('kafkajs')
const moment = require("moment-timezone");
const AthenaExpress = require("athena-express");
const s3 = new aws.S3();
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
async function getGroupInfos(targetDt) {
    const query = `SELECT region, user_language
                   FROM 
                        "${process.env.ATHENA_DB}".stat_content_summary_daily
                   WHERE 
                        create_date = '${targetDt}'
                   GROUP BY region, user_language`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getGroupInfos', err);
    }
}

// 일별 누적 매출 정보 조회
async function getContentSummary(targetDt, region, language) {
    let query = `
            SELECT 
                content_id,
                sum(cum_gmv) as cum_gmv
            FROM 
                "${process.env.ATHENA_DB}".stat_content_summary_daily
            WHERE 
                create_date = '${targetDt}'
            AND 
                region = '${region}'
            AND 
                user_language = '${language}'`;

    if(EXCLUDE_IDS[process.env.LOCALE].length > 0)
        query += ` AND content_id not in (${EXCLUDE_IDS[process.env.LOCALE].join(',')})`;

    query += `GROUP BY content_id
            ORDER BY cum_gmv DESC
            LIMIT 20`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getContentSummary', err);
    }
}

// 도메인 이벤트 생성
async function makeDomainEvent(region, language, items) {
    let ids = [];
    items.forEach(function (item, index) {
        ids.push(item.content_id);
    });
    const attribute = `{
                            "type": "STEADY_SELLER",
                            "ids": [${ids.join(',')}]
                        }`;
    const message = `{
                    "event": {
                        "type": "CREATE",
                        "attributes":[{
                            "region": "${region}",
                            "language": "${language}",
                            "list": [${attribute}]
                        }]
                    },
                    "created": "${moment.utc().format()}"
                }`;
    const trimedMessage = JSON.stringify(JSON.parse(message));
    console.info(`[Domain Message] - ${trimedMessage}`);
    return trimedMessage;
}

// 스테디 셀러 작품을 Kafka 도메인 이벤트로 발행한다.
async function publishDomainEvent(region, language, items) {
    const trimedMessage = await makeDomainEvent(region, language, items);
    await produceKafkaMessage(trimedMessage);
}

// Send Kafka Message
async function produceKafkaMessage(message) {
    try {
        // Setup kafka js
        const kafka = new Kafka({
            clientId: process.env.KAFKA_TOPIC,
            brokers: process.env.KAFKA_BROKER.split(",")
        })
        const producer = kafka.producer()
        await producer.connect()
        await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: [
                {value: message}
            ],
        })
        await producer.disconnect();
    } catch (err) {
        console.error('[ERROR] produceKafkaMessage', err);
    }
}

async function createPartitions(table) {
    // 최근 3일의 파티션 재 생성
    for (let i = 0; i < 3; i++) {
        let day = moment().utc().subtract(i, 'day').format('YYYY-MM-DD');
        let prefix = `${table}/create_date=${day}`;

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

module.exports = {
    getGroupInfos,
    getContentSummary,
    makeDomainEvent,
    publishDomainEvent,
    createPartitions
}
