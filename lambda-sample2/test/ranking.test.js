'use strict';
const ranking = require('../ranking');
const sinon = require("sinon");
const expect = require("chai").expect;
const HashMap = require("hashmap");
const summary = require("./datas/summary.json");

const targetDate = "2020-02-01-01";
const region = "ko";
const language = "kor";

const WEIGHTS = {
    "twn": [1.2, 1, 1.5],
    "tha": [1.2, 1, 1.5],
    "idn": [1.2, 1, 1.5],
    "kor": [1.2, 1, 2]
};

const sandbox = sinon.createSandbox();

describe('실시간 랭킹 생성 함수 테스트', () => {
    afterEach(() => {
        sandbox.restore();
    });

    it("통계데이터 스코어 정렬 기능이 정상적으로 동작한다.", async () => {
        let sortedSummary = ranking.sort('open_uu', summary.Items);
        let beforeScore = Number(sortedSummary[0].open_uu);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].open_uu));
            beforeScore = Number(sortedSummary[i].open_uu);
        }

        sortedSummary = ranking.sort('open_cnt', summary.Items);
        beforeScore = Number(sortedSummary[0].open_cnt);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].open_cnt));
            beforeScore = Number(sortedSummary[i].open_uu);
        }

        sortedSummary = ranking.sort('total_gmv', summary.Items);
        beforeScore = Number(sortedSummary[0].total_gmv);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].total_gmv));
            beforeScore = Number(sortedSummary[i].total_gmv);
        }
    });

    it("스코어별 순위에 가중치를 적용하는 기능이 정상적으로 동작한다.", async () => {
        let scoreBoard = new HashMap();
        let sortedSummary = ranking.sort("open_uu", summary.Items);
        const weight = WEIGHTS["kor"];
        sortedSummary.forEach(function (item, index) {
            if (item["open_uu"] <= 0) {
                ranking.caculateOverallScore(scoreBoard, item.content_id, 0);
            } else {
                ranking.caculateOverallScore(scoreBoard, item.content_id, (sortedSummary.length - index) * weight[0]);
            }
        });
        sortedSummary.forEach(function (item, index) {
            expect(scoreBoard.get(item.content_id)).to.be.eq((sortedSummary.length - index) * weight[0]);
        });
    });

    it("통합 점수 랭킹을 산정하는 함수가 정상적으로 동작한다.", async () => {
        process.env.LOCALE = language;
        const weight = WEIGHTS[language];
        const types = ["open_uu", "open_cnt", "total_gmv"];
        let scoreBoard = new HashMap();
        let sortedSummary;
        // 타입별로 작품의 점수를 산정하여 합산한다.
        types.forEach(function (type, index) {
            sortedSummary = ranking.sort(type, summary.Items);
            sortedSummary.forEach(function (item, index) {
                if (item[type] <= 0) {
                    ranking.caculateOverallScore(scoreBoard, item.content_id, 0);
                } else {
                    ranking.caculateOverallScore(scoreBoard, item.content_id, (sortedSummary.length - index) * weight[0]);
                }
            });
        });
        // 통합 점수를 total_score 필드에 기록한다
        sortedSummary.forEach(function (item) {
            item.total_score = scoreBoard.get(item.content_id);
        });
        // 통합 점수 기준으로 데이터를 정렬한다
        ranking.sort("total_score", sortedSummary);

        // 통합 점수를 기준으로 정렬한 데이터와 전체 랭킹을 산정하는 함수의 결과가 동일한지 확인
        const totalRanking = await ranking.makeTotalRanking(summary.Items);
        sortedSummary.forEach(function (item, index) {
            expect(item.total_score).to.be.eq(totalRanking[index].total_score);
        });
    });

    it("랭킹에 등락정보가 정상적으로 생성된다.", async () => {
        const totalRanking = await ranking.makeTotalRanking(summary.Items);
        const lastRanking = new HashMap();
        lastRanking.set(54, 10);
        lastRanking.set(40, 18);
        lastRanking.set(75, 45);
        lastRanking.set(60, 28);
        lastRanking.set(83, 5);
        lastRanking.set(22, 58);
        lastRanking.set(63, 32);
        lastRanking.set(51, 7);
        lastRanking.set(80, 70);
        lastRanking.set(42, 25);
        sandbox.stub(ranking, "getBeforeRankingDt").returns({Items: [{target_dt: targetDate}]});
        sandbox.stub(ranking, "getRankingChart").returns(lastRanking);
        const rankings = await ranking.setRankAndUpDownInfo(targetDate, region, language, totalRanking);
        for (let ranking of rankings) {
            if (lastRanking.has(ranking.content_id)) {
                expect(ranking.rank_last).to.be.eq(lastRanking.get(ranking.content_id));
            }
        }
    });

    it("랭킹정보로 올바른 포맷의 이벤트 메시지가 생성된다", async () => {
        const totalRankings = await ranking.makeTotalRanking(summary.Items);
        const lastRanking = new HashMap();
        lastRanking.set(54, 10);
        lastRanking.set(40, 18);
        lastRanking.set(75, 45);
        lastRanking.set(60, 28);
        lastRanking.set(83, 5);
        lastRanking.set(22, 58);
        lastRanking.set(63, 32);
        lastRanking.set(51, 7);
        lastRanking.set(80, 70);
        lastRanking.set(42, 25);

        sandbox.stub(ranking, "getBeforeRankingDt").returns({Items: [{target_dt: targetDate}]});
        sandbox.stub(ranking, "getRankingChart").returns(lastRanking);
        let rankings = await ranking.setRankAndUpDownInfo(targetDate, region, language,  totalRankings);
        let domainEvent = JSON.parse(await ranking.makeDomainEvent(region, language, rankings));
        expect(domainEvent.event.type).to.be.eq("CREATE");
        expect(domainEvent.event.attributes[0].region).to.be.eq(region);
        expect(domainEvent.event.attributes[0].language).to.be.eq(language);
        expect(domainEvent.event.attributes[0].list.length).to.be.eq(1);
        expect(domainEvent.event.attributes[0].list[0].type).to.be.eq("HOURLY");
        expect(domainEvent.event.attributes[0].list[0].genre_code).to.be.eq('ALL');
        expect(domainEvent.event.attributes[0].list[0].ranking.title).to.be.eq("REALTIME RANKING");
        expect(domainEvent.event.attributes[0].list[0].ranking.total).to.be.eq(78);
        expect(domainEvent.event.attributes[0].list[0].ranking.list.length).to.be.eq(78);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].content_id).to.be.eq(40);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].rank).to.be.eq(1);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].rank_last).to.be.eq(18);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[9].content_id).to.be.eq(22);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[9].rank).to.be.eq(10);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[9].rank_last).to.be.eq(58);
    });
});

