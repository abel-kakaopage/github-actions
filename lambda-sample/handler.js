'use strict';
const ranking = require('./ranking')
const moment = require("moment-timezone");

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.main = async event => {
    // 데이터 수집일자 - Custom으로 인자로 세팅하거나 1 전으로 세팅
    let targetDate = event.targetDate;
    if (!targetDate)
        targetDate = moment().tz(process.env.TIME_ZONE).add(-1, 'day').format('YYYY-MM-DD');
    // 데이터 파티션 생성
    await ranking.createPartitions('stat_content_summary_daily');
    await sleep(5000);
    const groupInfos = await ranking.getGroupInfos(targetDate);
    console.info(`[Generate Steady Seller] UTC - ${moment.utc()} / TIME_ZONE - ${moment().tz(process.env.TIME_ZONE)} / targetDate:${targetDate}`);
    // 리전의 언어 정보 조회
    if (groupInfos.Items && groupInfos.Items.length > 0) {
        for (const parentIndex in groupInfos.Items) {
            const region = groupInfos.Items[parentIndex].region;
            const language = groupInfos.Items[parentIndex].user_language;
            console.info(`Region:${region}, Language:${language}`);
            // 일별 매출 내역이 높은 순으로 작품을 조회한다.
            const summary = await ranking.getContentSummary(targetDate, region, language);
            if (summary.Items) {
                // 도메인 이벤트로 발행한다.
                await ranking.publishDomainEvent(region, language, summary.Items);
            }
        }
    } else {
        const msg = "[ERROR] Daily summary data not exists";
        console.error(msg);
    }
};
