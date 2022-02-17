'use strict';
const aws = require("aws-sdk");
aws.config.update({
    region: process.env.REGION
})
const {Kafka} = require('kafkajs')
const moment = require("moment");
const HashMap = require("hashmap");
const AthenaExpress = require("athena-express");
const {v4: uuid4} = require('uuid');
const s3 = new aws.S3();

const athenaExpressConfig = {
    aws,
    db: process.env.ATHENA_DB,
    s3: process.env.QUERY_RESULT,
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

const WEIGHTS = {
    "twn": [1.2, 1, 1.5],
    "tha": [1.2, 1, 1.5],
    "idn": [1.2, 1, 1.5],
    "kor": [1.2, 1, 2],
    "fra": [1.2, 1, 1.5]
}

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
                        "${process.env.ATHENA_DB}".stat_content_summary_hourly
                   WHERE 
                        create_dt = '${targetDt}'
                   GROUP BY region, user_language`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getGroupInfos', err);
    }
}

// 각 집계 항목에 비중을 적용하여 통계 정보를 조회한다.
async function getContentSummary(targetDt, region, language) {
    let query = `
            SELECT 
                content_id,
                genre_code,
                sum(open_uu) as open_uu, 
                sum(open_cnt) as open_cnt, 
                sum(total_gmv) as total_gmv
            FROM 
                "${process.env.ATHENA_DB}".stat_content_summary_hourly
            WHERE 
                create_dt = '${targetDt}'
            AND 
                region = '${region}'
            AND 
                user_language = '${language}'`;

    if(EXCLUDE_IDS[process.env.LOCALE].length > 0)
        query += ` AND content_id not in (${EXCLUDE_IDS[process.env.LOCALE].join(',')})`;

    query += ` GROUP BY content_id, genre_code`;

    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getContentSummary', err);
    }
}

// 열람자수/열람건수/매출 기준으로 랭킹을 매기기 위해 리스트를 정렬한다.
function sort(sort, items) {
    if (sort === "open_uu") {
        return items.sort(function (a, b) {
            return Number(a.open_uu) > Number(b.open_uu) ? -1 : Number(a.open_uu) < Number(b.open_uu) ? 1 : 0;
        });
    } else if (sort === "open_cnt") {
        return items.sort(function (a, b) {
            return Number(a.open_cnt) > Number(b.open_cnt) ? -1 : Number(a.open_cnt) < Number(b.open_cnt) ? 1 : 0;
        });
    } else if (sort === "total_gmv") {
        return items.sort(function (a, b) {
            return Number(a.total_gmv) > Number(b.total_gmv) ? -1 : Number(a.total_gmv) < Number(b.total_gmv) ? 1 : 0;
        });
    } else if (sort === "total_score") {
        return items.sort(function (a, b) {
            return Number(a.total_score) > Number(b.total_score) ? -1 : Number(a.total_score) < Number(b.total_score) ? 1 : 0;
        });
    }
}

// 열람자수/열람건수/매출 기준으로 랭킹 포인트를 합산하여 총 점수를 계산
function caculateOverallScore(scoreBoard, contentId, score) {
    if (scoreBoard.get(contentId)) {
        scoreBoard.set(contentId, scoreBoard.get(contentId) + score);
    } else {
        scoreBoard.set(contentId, score);
    }
}

// 랭킹 정보 저장
async function createRankingInfo(targetDt, uuid, region, language, rankingChart) {
    const query = `
            INSERT INTO "${process.env.ATHENA_DB}".ranking_info (target_dt, type, uuid, region, language, main_genre_code, total_count, created_dt)
            VALUES (
                '${targetDt}',
                'HOURLY',
                '${uuid}', 
                '${region}',
                '${language}',
                'ALL',
                ${rankingChart.length},
                date_parse('${moment.utc().format()}','%Y-%m-%dT%H:%i:%sZ')
            )`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] createRankingInfo', err);
    }
}

// 랭킹 항목 저장 - athena는 한번에 1000개까지만 insert가 가능하므로 데이터를 분할하여 insert 한다.
async function createRankingChart(targetDt, uuid, rankingChart) {
    try {
        let start = 0;
        let sliceSize = 1000;
        let total = rankingChart.length;
        let insertTotal = 0;
        if (total > 0) {
            for (let i = 0; i < total / sliceSize; i++) {
                let splitChart = rankingChart.slice(start, start + sliceSize);
                await splitInsertToAthena(targetDt, uuid, splitChart);
                console.info(`Inserted > Total : ${insertTotal += splitChart.length}`);
                start += sliceSize;
            }
        } else {
            console.info(`Insert data does not exist`);
        }
    } catch (err) {
        console.error("[ERROR] createRankingChart", err);
    }
}

// 아테나에 정보 INSERT
async function splitInsertToAthena(targetDt, uuid, rankingChart) {
    if (rankingChart) {
        let insertDatas = [];
        rankingChart.forEach(function (item) {
            insertDatas.push(`('${uuid}',${item.content_id},'${item.genre_code}',${item.rank},${item.rank_last},'${targetDt}')`);
        })
        const query = `INSERT INTO "${process.env.ATHENA_DB}".ranking_chart(uuid, content_id, main_genre_code, rank, rank_last, target_dt) VALUES ${insertDatas.join(',')}`;
        try {
            await athenaExpress.query(query);
        } catch (error) {
            console.error('[ERROR] splitInsertToAthena', error);
        }
    } else {
        console.info('Insert data does not exist');
    }
}

// 랭킹이 이미 존재하는지 체크
async function existChart(targetDt) {
    let query = `
            SELECT 1
            FROM "${process.env.ATHENA_DB}".ranking_chart chart
            INNER JOIN "${process.env.ATHENA_DB}".ranking_info info ON chart.uuid = info.uuid
            WHERE info.target_dt='${targetDt}'
            AND chart.target_dt='${targetDt}'
            AND info.type='HOURLY'
            LIMIT 1`;

    try {
        await athenaExpress.query(query).then(function (chart) {
            if (chart.Items)
                return true;
        })
        return false;
    } catch (error) {
        console.error('[ERROR] existChart', error);
    }
}

async function getBeforeRankingDt(targetDate, target5BeforeDate) {
    let query = `
        SELECT target_dt
        FROM "${process.env.ATHENA_DB}".ranking_info
        WHERE type = 'HOURLY'
        AND target_dt > '${target5BeforeDate}'
        AND target_dt < '${targetDate}'
        ORDER BY target_dt DESC LIMIT 1`;

    try {
        return await athenaExpress.query(query);
    } catch (error) {
        console.error('[ERROR] getBeforeRankingDt', error);
    }
}

// 순위 등락을 구하기 위해 이전 랭킹 리스트를 조회
async function getRankingChart(region, language, targetBeforeDate) {
    const query = `
            SELECT chart.content_id, chart.rank 
            FROM "${process.env.ATHENA_DB}".ranking_chart chart
            INNER JOIN "${process.env.ATHENA_DB}".ranking_info info ON chart.uuid = info.uuid
            WHERE info.target_dt = '${targetBeforeDate}'
            AND chart.target_dt = '${targetBeforeDate}'
            AND info.type='HOURLY'
            AND info.region='${region}'
            AND info.language='${language}'`;
    try {
        let rankingMap = new HashMap();
        await athenaExpress.query(query).then(function (chart) {
            if (chart.Items) {
                chart.Items.forEach(function (item) {
                    rankingMap.set(item['content_id'], item['rank']);
                });
            }
        })
        return rankingMap;
    } catch (error) {
        console.error('[ERROR] getRankingChart', error);
    }
}

// 전체 랭킹 생성
async function makeTotalRanking(summary) {
    let scoreBoard = new HashMap();
    const weight = WEIGHTS[process.env.LOCALE];
    // 열람자 수 기반 점수 계산
    let items = sort("open_uu", summary)
    console.debug("[sort by open_uu score]");
    items.forEach(function (item, index) {
        if (item.open_uu <= 0) {
            caculateOverallScore(scoreBoard, item.content_id, 0);
            console.debug(`${items.length - index}) content_id=${item.content_id} - open_uu : ${item.open_uu} - score : 0`);
        } else {
            caculateOverallScore(scoreBoard, item.content_id, (items.length - index) * weight[0]);
            console.debug(`${items.length - index}) content_id=${item.content_id} - open_uu : ${item.open_uu} - score : (${items.length}-${index}*${weight[0]})=${(items.length - index) * weight[0]}`);
        }
    });
    // 열람건 수 기반 점수 계산
    items = sort("open_cnt", summary)
    console.debug("[sort by open_cnt score]");
    items.forEach(function (item, index) {
        if (item.open_cnt <= 0) {
            caculateOverallScore(scoreBoard, item.content_id, 0);
            console.debug(`${items.length - index}) content_id=${item.content_id} - open_cnt : ${item.open_cnt} - score : 0`);
        } else {
            caculateOverallScore(scoreBoard, item.content_id, (items.length - index) * weight[1]);
            console.debug(`${items.length - index}) content_id=${item.content_id} - open_cnt : ${item.open_cnt} - score : (${items.length}-${index}*${weight[1]})=${(items.length - index) * weight[1]}`);
        }
    });
    // 매출액 기반 점수 계산
    items = sort("total_gmv", summary)
    console.debug("[sort by total_gmv score]");
    items.forEach(function (item, index) {
        if (item.total_gmv <= 0) {
            caculateOverallScore(scoreBoard, item.content_id, 0);
            console.debug(`${items.length - index}) content_id=${item.content_id} - total_gmv : ${item.total_gmv} - score : 0`);
        } else {
            caculateOverallScore(scoreBoard, item.content_id, (items.length - index) * weight[2]);
            console.debug(`${items.length - index}) content_id=${item.content_id} - total_gmv : ${item.total_gmv} - score : (${items.length}-${index}*${weight[2]})=${(items.length - index) * weight[2]}`);
        }
    });
    // 위에서 계산한 점수를 모두 더하여 통합 점수를 산정
    items.forEach(function (item) {
        item.total_score = scoreBoard.get(item.content_id);
    });
    // 통합 점수 기준으로 데이터 정렬
    return sort("total_score", items);
}

// 랭킹 정보에 등락정보 추가
async function setRankAndUpDownInfo(targetDate, region, language, totalRankings) {
    // 등락 정보 구성을 위해 최근 생성된 랭킹을 조회한다.
    let targetBeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-1, 'hour').format('YYYY-MM-DD-HH');
    const target5BeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-5, 'hour').format('YYYY-MM-DD-HH');
    const beforeRanking = await this.getBeforeRankingDt(targetDate, target5BeforeDate);
    if (beforeRanking && beforeRanking.Items && beforeRanking.Items.length > 0)
        targetBeforeDate = beforeRanking.Items[0].target_dt;
    const lastRanking = await this.getRankingChart(region, language, targetBeforeDate);
    totalRankings.forEach(function (item, index) {
        let rankingLast = 0;
        if (lastRanking)
            rankingLast = lastRanking.get(Number(item.content_id)) | 0;
        item.rank = index + 1;
        item.rank_last = rankingLast;
        console.info(`rank = ${item.rank}, content_id=${item.content_id}, genre=${item.genre_code}, total_score=${item.total_score}, rank_last=${item.rank_last}`)
    });
    return totalRankings;
}

// 생성한 랭킹 정보를 Athena DB에 저장
async function createRankingData(targetDate, region, language, totalRankings) {
    // 랭킹 정보를 athena에 저장
    const uuid = uuid4();
    await createRankingInfo(targetDate, uuid, region, language, totalRankings).then(async function () {
        await createRankingChart(targetDate, uuid, totalRankings);
    });
}

// 도메인 이벤트 생성
async function makeDomainEvent(region, language, rankingChart) {
    let list = [];
    rankingChart.some((item) => {
        // top 100에 처음 들어온경우 NEW 처리
        if(item.rank <= 100 && item.rank_last > 100) {
            item.rank_last = 0;
        }
        list.push(`{
                        "content_id": ${item.content_id},
                        "rank": ${item.rank},
                        "rank_last": ${item.rank_last}
                    }`);
        // 랭킹은 최대 100개 까지만 전달
        if (list.length === 100)
            return true;
    });

    let title = "실시간 랭킹";
    if(language !== "ko")
        title = "REALTIME RANKING";
    const attribute = `{
                            "type": "HOURLY",
                            "genre_code": "ALL",
                            "ranking": {
                               "title":"${title}",
                               "total":${list.length},
                               "list":[${list}]
                            }
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

// 랭킹을 Kafka 도메인 이벤트로 발행한다.
async function publishDomainEvent(region, language, rankingChart) {
    const trimedMessage = await makeDomainEvent(region, language, rankingChart);
    await produceKafkaMessage(trimedMessage);
}

// Send Kafka Message
async function produceKafkaMessage(message) {
    try {
        // Setup kafka js
        const kafka = new Kafka({
            clientId: 'generate-hourly-ranking',
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

// Athena는 쿼리로 데이터를 삭제할 수 없어 직접 bucket의 데이터를 삭제한다.
async function emptyS3Bucket(prefix) {
    let params = {
        Bucket: process.env.S3_BUCKET_NAME,
        Prefix: prefix
    }
    const listedObjects = await s3.listObjects(params).promise();
    if (listedObjects.Contents.length < 1) {
        console.info("S3 Object does not exist");
        return;
    }

    const deleteParams = {
        Bucket: process.env.S3_BUCKET_NAME,
        Delete: {Objects: []}
    };

    let dataExist = false;
    listedObjects.Contents.forEach(({Key}) => {
        if (Key !== process.env.S3_BUCKET_PREFIX) {
            deleteParams.Delete.Objects.push({Key});
            dataExist = true;
        }
    });
    if (dataExist) {
        console.info("Delete bucket", deleteParams.Delete);
        await s3.deleteObjects(deleteParams).promise();
        if (listedObjects.IsTruncated) await emptyS3Bucket();
    } else {
        console.info("Not exist delete Bucket")
    }
    return dataExist;
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

module.exports = {
    sort,
    existChart,
    emptyS3Bucket,
    createPartitions,
    getGroupInfos,
    getRankingChart,
    getBeforeRankingDt,
    getContentSummary,
    makeTotalRanking,
    setRankAndUpDownInfo,
    createRankingData,
    makeDomainEvent,
    caculateOverallScore,
    publishDomainEvent
}
