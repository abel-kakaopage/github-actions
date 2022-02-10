const lambda = require('../handler');
const sinon = require("sinon");
const expect = require("chai").expect;

describe('메인 함수 Validation 테스트', () => {

    it("메인함수가 성공적으로 실행된다.", async () => {
        const result = await lambda.main();
        expect(result.statusCode).to.equal(200);
    });
});
