const ranking = require('../ranking');
const sinon = require("sinon");
const expect = require("chai").expect;
const summaryByOpenCnt = require("./datas/summary-opencnt.json");
const summaryByFavoriteCnt = require("./datas/summary-favoritecnt.json");

describe('오늘의 매칭 생성 람다 함수 테스트', () => {

    it("오늘의 매칭 이벤트 메시지가 정상적으로 생성된다.", async () => {
        const region  = "kor";
        const language  = "ko";
        let domainEvent = JSON.parse(await ranking.makeDomainEvent(region, language, summaryByOpenCnt, summaryByFavoriteCnt));
        expect(domainEvent.event.type).to.be.eq("CREATE");
        expect(domainEvent.event.attributes[0].region).to.be.eq(region);
        expect(domainEvent.event.attributes[0].language).to.be.eq(language);
        expect(domainEvent.event.attributes[0].list.length).to.be.eq(2);
        expect(domainEvent.event.attributes[0].list[0].type).to.be.eq("TODAY_READ");
        expect(domainEvent.event.attributes[0].list[0].contents.title).to.be.eq("오늘 사람들이 가장 많이 본 작품들");
        expect(domainEvent.event.attributes[0].list[0].contents.ids).to.eql([71,16,29,69,34,55,14,82,61,12,81,65,25,53,44,76,39,84,70,51]);
        expect(domainEvent.event.attributes[0].list[1].type).to.be.eq("TODAY_SUBSCRIPTION");
        expect(domainEvent.event.attributes[0].list[1].contents.title).to.eq("오늘 사람들이 가장 많이 찜한 작품들");
        expect(domainEvent.event.attributes[0].list[1].contents.ids).to.eql([79,81,85,80,43,21,78,42,31,82,72,64,14,26,52,68,18,87,40,23]);
    });

    it("오늘의 매칭 이벤트 다국어 처리 메시지가 정상적으로 생성된다.", async () => {
        const region  = "twn";
        const language  = "tw";
        let domainEvent = JSON.parse(await ranking.makeDomainEvent(region, language, summaryByOpenCnt, summaryByFavoriteCnt));
        expect(domainEvent.event.type).to.be.eq("CREATE");
        expect(domainEvent.event.attributes[0].region).to.be.eq(region);
        expect(domainEvent.event.attributes[0].language).to.be.eq(language);
        expect(domainEvent.event.attributes[0].list.length).to.be.eq(2);
        expect(domainEvent.event.attributes[0].list[0].type).to.be.eq("TODAY_READ");
        expect(domainEvent.event.attributes[0].list[0].contents.title).to.be.eq("Most popular titles today");
        expect(domainEvent.event.attributes[0].list[0].contents.ids).to.eql([71,16,29,69,34,55,14,82,61,12,81,65,25,53,44,76,39,84,70,51]);
        expect(domainEvent.event.attributes[0].list[1].type).to.be.eq("TODAY_SUBSCRIPTION");
        expect(domainEvent.event.attributes[0].list[1].contents.title).to.eq("Most favorited titles today");
        expect(domainEvent.event.attributes[0].list[1].contents.ids).to.eql([79,81,85,80,43,21,78,42,31,82,72,64,14,26,52,68,18,87,40,23]);
    });
});

