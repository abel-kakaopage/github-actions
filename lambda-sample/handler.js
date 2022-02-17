'use strict';
const moment = require("moment-timezone");
const ranking = require("./ranking");
const RANGKING_TYPE = "DAILY_DAUM_PORTAL";

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.main = async event => {
    // 랭킹 수집일자 - Custom으로 인자로 세팅하거나 전일자로 세팅
    let targetDate = event.targetDate;
    let isManual = true;
    if (!targetDate) {
        targetDate = moment().tz(process.env.TIME_ZONE).add(-1, 'day').format('YYYY-MM-DD');
        isManual = false;
    }
    // 전전일자 랭킹과 등락 비교가 필요하여 targetDate 기준 1일전 날짜를 구한다.
    const targetBeforeDate = moment(targetDate, 'YYYY-MM-DD').add(-1, 'day').format('YYYY-MM-DD');
    // 수동 처리이거나 기존에 챠트가 생성되지 않은 경우에만 작업 진행
    let existsChart = await ranking.existChart(targetDate);
    console.info(`[Generate Daily Daum Portal Ranking] UTC - ${moment.utc()} / TIME_ZONE - ${moment().tz(process.env.TIME_ZONE)} => targetDate : ${targetDate}, targetBeforeDate : ${targetBeforeDate}, isManual : ${isManual}, existsChart : ${existsChart}`);
    if (isManual || !existsChart) {
        // 동일 요청이 들어올경우에 기존 데이터를 삭제한다.
        await ranking.emptyS3Bucket(`ranking_info/type=${RANGKING_TYPE}/target_dt=${targetDate}/`);
        await ranking.emptyS3Bucket(`ranking_chart/target_dt=${RANGKING_TYPE}-${targetDate}/`);
        // 데이터 파티션 생성
        await ranking.createPartitions('stat_content_summary_daily');
        await sleep(5000);
        // 리전의 언어 정보 조회
        const groupInfos = await ranking.getGroupInfos(targetDate);
        if (groupInfos.Items && groupInfos.Items.length > 0) {
            const rankingType = ["SERIAL", "FINISH"];
            // 리전의 언어별로 루프를 돌며 랭킹을 생성
            for (const parentIndex in groupInfos.Items) {
                // 언어 -> 장르별 랭킹 생성을 위한 루프
                for (const typeIndex in rankingType) {
                    const region = groupInfos.Items[parentIndex].region;
                    const language = groupInfos.Items[parentIndex].user_language;
                    const summary = await ranking.getContentSummary(targetDate, region, language, rankingType[typeIndex]);
                    console.info(`##### Region : ${region}, Language: ${language}, Genre : ${rankingType[typeIndex]}`)
                    if (summary.Items) {
                        // 랭킹 생성
                        let totalRankings = await ranking.makeTotalRanking(summary.Items)
                        // 이전 랭킹 정보와 비교하여 등락 정보를 생성
                        totalRankings = await ranking.setRankAndUpDownInfo(region, language, rankingType[typeIndex], totalRankings)
                        // 랭킹 정보 저장
                        await ranking.createRankingData(targetDate, region, language, rankingType, totalRankings)
                        // 도메인 이벤트 발행
                        await ranking.publishDomainEvent(region, language, rankingType[typeIndex], totalRankings);
                    }
                }
            }
        } else {
            const msg = "[ERROR] Daily summary data not exists(Daum Portal Ranking)";
            console.info(msg);
        }
    }
};

