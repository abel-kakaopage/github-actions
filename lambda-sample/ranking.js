'use strict';
const aws = require("aws-sdk");
aws.config.update({
    region: process.env.REGION
})
const {Kafka} = require('kafkajs')
const moment = require("moment-timezone");
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
    "kor": [1.2, 1, 2]
}

const EXCLUDE_IDS = {
    "twn": [],
    "tha": [],
    "idn": [],
    "kor": [877]
}

// Get Ranking Target Group : 리전의 Language별로 랭킹을 만들어야 함
async function getGroupInfos(targetDt) {
    const query = `SELECT region, user_language
                   FROM "${process.env.ATHENA_DB}".stat_content_summary_hourly
                   WHERE create_dt = '${targetDt}'
                   GROUP BY region, user_language`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getGroupInfos', err);
    }
}

// Get Genres : 일간 랭킹은 장르별 랭킹도 만들어야 함
async function getGenres(targetDt) {
    const query = `SELECT genre_code
                   FROM "${process.env.ATHENA_DB}".stat_content_summary_hourly
                   WHERE create_dt = '${targetDt}'
                   GROUP BY genre_code
                   ORDER BY genre_code asc`;
    try {
        return await athenaExpress.query(query);
    } catch (err) {
        console.error('[ERROR] getGenres', err);
    }
}

// 각 집계 항목의 통계 정보를 조회한다.
async function getContentSummary(targetDt, region, language, mainGenreCode) {
    let query = `
        SELECT content_id,
               genre_code,
               sum(open_uu)   as open_uu,
               sum(open_cnt)  as open_cnt,
               sum(total_gmv) as total_gmv
        FROM "${process.env.ATHENA_DB}".stat_content_summary_hourly
        WHERE create_dt = '${targetDt}'
          AND region = '${region}'
          AND user_language = '${language}'`;

    if (mainGenreCode !== "ALL")
        query += ` AND genre_code = '${mainGenreCode}'`;

    if (EXCLUDE_IDS[process.env.LOCALE].length > 0)
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
async function createRankingInfo(targetDt, uuid, region, language, genre, rankingChart) {
    const query = `
        INSERT INTO "${process.env.ATHENA_DB}".ranking_info (target_dt, type, uuid, region, language, main_genre_code,
                                                             total_count, created_dt)
        VALUES ('${targetDt}',
                'HOURLY',
                '${uuid}',
                '${region}',
                '${language}',
                '${genre}',
                ${rankingChart.length},
                date_parse('${moment.utc().format()}', '%Y-%m-%dT%H:%i:%sZ'))`;
    try {
        console.info(`createRankingInfo > ${targetDt} - ${region} - ${language} - ${genre}`);
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
                console.info(`createRankingChart > uuid : ${uuid} -Total : ${insertTotal += splitChart.length}`);
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
        const query = `INSERT INTO "${process.env.ATHENA_DB}".ranking_chart(uuid, content_id, main_genre_code, rank, rank_last, target_dt)
                       VALUES ${insertDatas.join(',')}`;
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
        FROM "${process.env.ATHENA_DB}".ranking_chart chart INNER JOIN "${process.env.ATHENA_DB}".ranking_info info ON chart.uuid = info.uuid
        WHERE info.target_dt = '${targetDt}'
        AND chart.target_dt = '${targetDt}'
        AND info.type = 'HOURLY'
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

// 랭킹 등락 비교를 위해 가장 최근에 랭킹 생성된 일자 조회
async function getBeforeRankingDt(targetDate, target5BeforeDate, mainGenreCode) {
    let query = `
        SELECT target_dt
        FROM "${process.env.ATHENA_DB}".ranking_info
        WHERE type = 'HOURLY'
          AND target_dt > '${target5BeforeDate}'
          AND target_dt < '${targetDate}'
          AND main_genre_code = '${mainGenreCode}'
        ORDER BY target_dt DESC LIMIT 1`;

    try {
        return await athenaExpress.query(query);
    } catch (error) {
        console.error('[ERROR] getBeforeRankingDt', error);
    }
}

// 이전 랭킹 리스트 조회(전체, 장르별)
async function getRankingChart(region, language, mainGenreCode, targetBeforeDate) {
    let query = `
        SELECT chart.content_id, chart.rank
        FROM "${process.env.ATHENA_DB}".ranking_chart chart
                 INNER JOIN "${process.env.ATHENA_DB}".ranking_info info ON chart.uuid = info.uuid
        WHERE info.target_dt = '${targetBeforeDate}'
        AND chart.target_dt = '${targetBeforeDate}'
        AND info.type = 'HOURLY'
        AND info.region = '${region}'
        AND info.language = '${language}'
        AND info.main_genre_code = '${mainGenreCode}'`;

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

// 도메인 이벤트 생성
async function makeDomainEvent(region, language, mainGenreCode, rankingChart) {
    let list = [];
    rankingChart.some((item) => {
        // top 100에 처음 들어온경우 NEW 처리
        if (item.rank <= 100 && item.rank_last > 100) {
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
    if (language !== "ko")
        title = "HOURLY RANKING";
    const attribute = `{
                            "type": "HOURLY",
                            "genre_code": "${mainGenreCode}",
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

// 도메인 이벤트 발행
async function produceKafkaMessage(message) {
    try {
        // Setup kafka js
        const kafka = new Kafka({
            clientId: 'generate-hourly_kor-ranking',
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

// 통계 데이터를 조회하기 위해 파티션 생성
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

// 파티션 생성 쿼리 실행
async function createPartition(partition, table) {
    let query = `ALTER TABLE ${process.env.ATHENA_DB}.${table}
        ADD IF NOT EXISTS PARTITION (${partition})`;
    try {
        await athenaExpress.query(query).then(function () {
            console.info(`[SUCC] createPartition - ${query}`);
        });
    } catch (error) {
        console.error(`[ERROR] createPartition : ${query} - ${error}`);
    }
}

const sleep = (ms) => {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}

// 전체 랭킹 생성
async function makeTotalRanking(targetDate, region, language) {
    let scoreBoard = new HashMap();
    const weight = WEIGHTS[process.env.LOCALE];
    const summary = await this.getContentSummary(targetDate, region, language, 'ALL');
    console.info(`##### Region : ${region}, Language: ${language}`)
    if (summary.Items) {
        // 열람자 수 기반 점수 계산
        let items = sort("open_uu", summary.Items)
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
        items = sort("open_cnt", summary.Items)
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
        items = sort("total_gmv", summary.Items)
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
    return null;
}

// 전체 랭킹에서 장르별 랭킹 추출
function makeGenreRanking(totalRankings, genres) {
    const rankingMap = new Map();
    if(totalRankings) {
        rankingMap.set('ALL', totalRankings);
        for (const genreIndex in genres.Items) {
            const genreCode = genres.Items[genreIndex].genre_code;
            let subRankings = [];
            totalRankings.forEach(function (ranking, index) {
                if(ranking.genre_code === genreCode) {
                    subRankings.push(ranking);
                }
            });
            rankingMap.set(genreCode, subRankings);
        }
    }
    return rankingMap;
}

// 랭킹 정보에 등락정보 추가
async function setRankAndUpDownInfo(targetDate, region, language, rankingMap) {
    // 등락 정보 구성을 위해 최근 생성된 랭킹을 조회한다.
    let targetBeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-1, 'hour').format('YYYY-MM-DD-HH');
    const target5BeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-5, 'hour').format('YYYY-MM-DD-HH');
    const rankingInfoMap = new Map();
    if(rankingMap && rankingMap.size > 0) {
        for (let ranking of rankingMap) {
            const genre = ranking[0];
            const beforeRanking = await this.getBeforeRankingDt(targetDate, target5BeforeDate, genre);
            if (beforeRanking && beforeRanking.Items && beforeRanking.Items.length > 0)
                targetBeforeDate = beforeRanking.Items[0].target_dt;
            const lastRanking = await this.getRankingChart(region, language, genre, targetBeforeDate);
            ranking[1].forEach(function (item, index) {
                let rankingLast = 0;
                if (lastRanking)
                    rankingLast = lastRanking.get(Number(item.content_id)) | 0;

                item.rank = index + 1;
                item.rank_last = rankingLast;
            });
            rankingInfoMap.set(genre, JSON.stringify(ranking[1]));
        }
    }
    return rankingInfoMap;
}

// 생성한 랭킹 정보를 Athena DB에 저장
async function createRankingData(targetDate, region, language, rankingInfoMap) {
    if(rankingInfoMap && rankingInfoMap.size > 0) {
        for (let rankingInfos of rankingInfoMap) {
            const genre = rankingInfos[0];
            const items = JSON.parse(rankingInfos[1]);
            // 랭킹 정보를 athena에 저장
            const uuid = uuid4();
            await createRankingInfo(targetDate, uuid, region, language, genre, items).then(async function () {
                await createRankingChart(targetDate, uuid, items);
            });
        }
    }
}

// 전체, 장르별 랭킹 이벤트 발행
async function publishRanking(region, language, rankingInfoMap) {
    if(rankingInfoMap && rankingInfoMap.size > 0) {
        for (let rankingInfos of rankingInfoMap) {
            const genre = rankingInfos[0];
            const items = JSON.parse(rankingInfos[1]);
            // 장르별 도메인 메시지 발행
            const eventMessage = await makeDomainEvent(region, language, genre, items);
            await produceKafkaMessage(eventMessage);
        }
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
    caculateOverallScore,
    getGenres,
    makeTotalRanking,
    makeGenreRanking,
    setRankAndUpDownInfo,
    createRankingData,
    makeDomainEvent,
    publishRanking
}
