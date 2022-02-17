const ranking = require('../ranking');
const sinon = require("sinon");
const expect = require("chai").expect;
const summary = require("./datas/summary.json");

const region = "ko";
const language = "kor";

describe('스테디 셀러 생성 람다 함수 테스트', () => {

    it("스테디 셀러 이벤트 메시지가 정상적으로 생성된다.", async () => {
        let domainEvent = JSON.parse(await ranking.makeDomainEvent(region, language, summary.Items));
        expect(domainEvent.event.type).to.be.eq("CREATE");
        expect(domainEvent.event.attributes[0].region).to.be.eq(region);
        expect(domainEvent.event.attributes[0].language).to.be.eq(language);
        expect(domainEvent.event.attributes[0].list.length).to.be.eq(1);
        expect(domainEvent.event.attributes[0].list[0].type).to.be.eq("STEADY_SELLER");
        expect(domainEvent.event.attributes[0].list[0].ids).to.eql([59, 157, 126, 266, 35, 168, 257, 134, 248, 214, 142, 82, 288, 179, 77, 97, 280, 27, 48, 108]);
    });
});
