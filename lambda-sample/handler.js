'use strict';
const moment = require("moment");
const ranking = require("./ranking");

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.main = async event => {
    // 랭킹 수집일자 - Custom으로 인자로 세팅하거나 전주 마지막 일요일로 생성
    let beforeWeekLastDay = moment().utc().endOf('week').add(-1, 'week').add(1, 'day').format('YYYY-MM-DD');
    let isManual = false;
    if (event.beforeWeekLastDay) {
        beforeWeekLastDay = event.beforeWeekLastDay;
        isManual = true;
    }

    let existsChart = await ranking.existChart(beforeWeekLastDay);
    console.info(`[Generate Weekly Ranking] ${moment.utc()} => beforeWeekLastDay: ${beforeWeekLastDay}, isManual : ${isManual}, existsChart : ${existsChart}`);
    if (isManual || !existsChart) {
        // 동일 요청이 들어올경우에 기존 데이터를 삭제한다.
        await ranking.emptyS3Bucket(`ranking_info/type=WEEKLY/target_dt=${beforeWeekLastDay}/`);
        await ranking.emptyS3Bucket(`ranking_chart/target_dt=WEEKLY-${beforeWeekLastDay}/`);
        // 데이터 파티션 생성
        await ranking.createPartitions('stat_content_summary_weekly');
        await sleep(5000);
        // 리전의 언어 정보 조회
        const groupInfos = await ranking.getGroupInfos(beforeWeekLastDay);
        if (groupInfos.Items && groupInfos.Items.length > 0) {
            for (const parentIndex in groupInfos.Items) {
                const region = groupInfos.Items[parentIndex].region;
                const language = groupInfos.Items[parentIndex].user_language;
                const summary = await ranking.getContentSummary(beforeWeekLastDay, region, language);
                if (summary.Items) {
                    // 전체 랭킹 생성
                    let totalRankings = await ranking.makeTotalRanking(summary.Items)
                    // 이전 랭킹 정보와 비교하여 등락 정보를 생성
                    totalRankings = await ranking.setRankAndUpDownInfo(region, language, totalRankings);
                    // 랭킹 정보를 저장
                    await ranking.createRankingData(beforeWeekLastDay, region, language, totalRankings);
                    // 도메인 이벤트 발행
                    await ranking.publishDomainEvent(region, language, totalRankings);
                }
            }
        } else {
            const msg = "[ERROR] Weekly summary data not exists";
            console.error(msg);
        }
    }
}
