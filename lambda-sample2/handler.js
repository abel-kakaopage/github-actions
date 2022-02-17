'use strict';
const moment = require("moment-timezone");
const ranking = require("./ranking");

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
    // 수동 처리이거나 기존에 챠트가 생성되지 않은 경우에만 작업 진행
    let existsChart = await ranking.existChart(targetDate);
    console.info(`[Generate Daily Ranking] UTC - ${moment.utc()} / TIME_ZONE - ${moment().tz(process.env.TIME_ZONE)} => targetDate : ${targetDate}, isManual : ${isManual}, existsChart : ${existsChart}`);
    if (isManual || !existsChart) {
        // 동일 요청이 들어올경우에 기존 데이터를 삭제한다.
        await ranking.emptyS3Bucket(`ranking_info/type=DAILY/target_dt=${targetDate}/`);
        await ranking.emptyS3Bucket(`ranking_chart/target_dt=${targetDate}/`);
        // 데이터 파티션 생성
        await ranking.createPartitions('stat_content_summary_daily');
        await sleep(5000);
        // 리전의 언어 정보 조회
        const groupInfos = await ranking.getGroupInfos(targetDate);
        if (groupInfos.Items && groupInfos.Items.length > 0) {
            // 장르 조회
            const genres = await ranking.getGenres(targetDate);
            // 리전의 언어별로 루프를 돌며 랭킹을 생성
            for (const parentIndex in groupInfos.Items) {
                // 언어 -> 장르별 랭킹 생성을 위한 루프
                const region = groupInfos.Items[parentIndex].region;
                const language = groupInfos.Items[parentIndex].user_language;
                // 전체 랭킹 생성
                const totalRankings = await ranking.makeTotalRanking(targetDate, region, language);
                if(totalRankings) {
                    // 장르별 랭킹 생성
                    const rankingMap = ranking.makeGenreRanking(totalRankings, genres);
                    // 등락 정보 생성
                    const rankingInfoMap = await ranking.setRankAndUpDownInfo(targetDate, region, language, rankingMap);
                    // 랭킹 정보를 athena에 저장
                    await ranking.createRankingData(targetDate, region, language, rankingInfoMap);
                    // 이벤트 메시지 발행
                    await ranking.publishRanking(region, language, rankingInfoMap);
                }
            }
        } else {
            const msg = "[ERROR] Daily summary data not exists";
            console.error(msg);
        }
    }
};

