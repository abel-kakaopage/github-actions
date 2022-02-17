'use strict';
const moment = require("moment");
const ranking = require("./ranking");

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.main = async event => {
    // 랭킹 수집일자 - Custom으로 인자로 세팅하거나 1시간 전으로 세팅
    let targetDate = event.targetDate;
    let isManual = true;
    if (!targetDate) {
        targetDate = moment().utc().add(-1, 'hour').format('YYYY-MM-DD-HH');
        isManual = false;
    }
    // 전전 시간의 랭킹과 등락 비교가 필요하여 targetDate 기준 1시간전 날짜를 구한다.
    let targetBeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-1, 'hour').format('YYYY-MM-DD-HH');
    let existsChart = await ranking.existChart(targetDate);
    console.info(`[Generate Hourly Ranking] ${moment.utc()} => targetDate : ${targetDate}, targetBeforeDate : ${targetBeforeDate}, isManual : ${isManual}, existsChart : ${existsChart}`);
    if(isManual || !existsChart) {
        // 동일 요청이 들어올경우에 기존 데이터를 삭제한다.
        await ranking.emptyS3Bucket(`ranking_info/type=HOURLY/target_dt=${targetDate}/`);
        await ranking.emptyS3Bucket(`ranking_chart/target_dt=${targetDate}/`);
        // 데이터 파티션 생성
        await ranking.createPartitions('stat_content_summary_hourly');
        await sleep(5000);
        // 리전의 언어 정보 조회
        const groupInfos = await ranking.getGroupInfos(targetDate);
        if (groupInfos.Items && groupInfos.Items.length > 0) {
            for (const parentIndex in groupInfos.Items) {
                const region = groupInfos.Items[parentIndex].region;
                const language = groupInfos.Items[parentIndex].user_language;
                const summary = await ranking.getContentSummary(targetDate, region, language);
                if (summary.Items) {
                    // 전체 랭킹 생성
                    let totalRankings = await ranking.makeTotalRanking(summary.Items)
                    // 이전 랭킹 정보와 비교하여 등락 정보를 생성
                    totalRankings = await ranking.setRankAndUpDownInfo(targetDate, region, language, totalRankings)
                    // 랭킹 정보를 저장
                    await ranking.createRankingData(targetDate, region, language, totalRankings)
                    // 도메인 이벤트 발행
                    await ranking.publishDomainEvent(region, language, totalRankings);
                }
            }
        } else {
            const msg = "[ERROR] Hourly summary data not exists";
            console.error(msg);
        }
    }
};
