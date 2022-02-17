'use strict';
const ranking = require("./ranking");
const moment = require("moment");

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.main = async event => {
    // 데이터 수집일자 - Custom으로 인자로 세팅하거나 현재 시간으로 세팅
    let targetDate = event.targetDate;
    if (!targetDate)
        targetDate = moment().utc().format('YYYY-MM-DD-HH');
    // 지난 24시간 데이터를 조회할수 있도록 지난 24시간 날짜 정보를 구한다.
    const targetBeforeDate = moment(targetDate, 'YYYY-MM-DD-HH').add(-24, 'hour').format('YYYY-MM-DD-HH');
    console.info(`targetStartDate: ${targetBeforeDate}, targetEndDate: ${targetDate}`);
    // 데이터 파티션 생성
    await ranking.createPartitions('stat_content_summary_hourly');
    await sleep(5000);
    // 리전의 언어 정보 조회
    const groupInfos = await ranking.getGroupInfos(targetBeforeDate, targetDate);
    if (groupInfos.Items && groupInfos.Items.length > 0) {
        for (const parentIndex in groupInfos.Items) {
            const region = groupInfos.Items[parentIndex].region;
            const language = groupInfos.Items[parentIndex].user_language;
            console.info(`TargetDate:${targetDate}, Region:${region}, Language:${language}`);
            // 많이 본 작품 순 조회
            const summaryByOpenCnt = await ranking.getContentSummary(targetBeforeDate, targetDate, region, language, "open_cnt");
            // 많이 찜한 작품 순 조회
            const summaryByFavoriteCnt = await ranking.getContentSummary(targetBeforeDate, targetDate, region, language, "favorite_cnt");
            if(summaryByOpenCnt && summaryByFavoriteCnt) {
                // 도메인 이벤트 메시지 구성
                const message = await ranking.makeDomainEvent(region, language, summaryByOpenCnt, summaryByFavoriteCnt);
                // 도메인 이벤트 메시지 발행
                await ranking.produceKafkaMessage(message);
            }
        }
    } else {
        const msg = "[ERROR] Hourly summary data not exists";
        console.error(msg);
    }
};
