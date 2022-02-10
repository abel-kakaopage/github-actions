const lambda = require('../handler');
const AWS = require('aws-sdk');
const sinon = require("sinon");
const expect = require("chai").expect;
const eventJson = require('./input-data/update-push-test.json');
let event = eventJson;
const {GenericContainer} = require("testcontainers");
const mysql = require("mysql");
const dbModule = require('../db-module');

describe('메인 함수 Validation 테스트', () => {
    const containers = {};
    before(async () => {
        containers.mysqlContainer = await new GenericContainer('mysql:5.7')
            .withExposedPorts(3306)
            .withEnv('MYSQL_ALLOW_EMPTY_PASSWORD', '1')
            .withEnv('MYSQL_DATABASE', 'popularity')
            .start();

        let sql = "CREATE TABLE `subscription` (" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT," +
            "  `user_id` varchar(45) CHARACTER SET latin1 NOT NULL," +
            "  `content_id` bigint(20) NOT NULL," +
            "  `notification_enabled_flag` tinyint(1) NOT NULL DEFAULT '1'," +
            "  `created_dt` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)," +
            "  `updated_dt` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)," +
            "  PRIMARY KEY (`id`)," +
            "  UNIQUE KEY `udx_UserId_ContentId` (`user_id`,`content_id`)," +
            "  KEY `idx_ContentId_NotificationEnabledFlag` (`content_id`,`notification_enabled_flag`)" +
            ") ENGINE=InnoDB AUTO_INCREMENT=573892 DEFAULT CHARSET=utf8mb4;"
        await executeQuery(sql, []);

        sql = "INSERT INTO `subscription`" +
            "(`user_id`," +
            "`content_id`," +
            "`notification_enabled_flag`," +
            "`created_dt`," +
            "`updated_dt`)" +
            "VALUES" +
            "(?,?,?,now(),now())"
        await executeQuery(sql, ['koru56d69b530de080', 66, true]);
        await executeQuery(sql, ['koru56d79b530de081', 66, true]);
        await executeQuery(sql, ['koru4440fb9284edb9', 66, true]);
        await executeQuery(sql, ['koru386bc18f406996', 66, true]);
        await executeQuery(sql, ['kru22abf9acf77da2', 66, true]);
    });

    after(async () => {
    });

    async function getConnection() {
        return mysql.createConnection({
            host: containers.mysqlContainer.getHost(),
            port: containers.mysqlContainer.getMappedPort(3306),
            user: 'root',
            database: 'popularity'
        });
    }

    function executeQuery(sql, values) {
        return new Promise(async (resolve, reject) => {
            const connection = await getConnection();
            connection.query(sql, values, function (err, result) {
                if (result)
                    resolve(result);
                if (err) {
                    console.error("[Error] executeQuery", err);
                    reject(err);
                }
            });
            connection.end(function (err) {
            });
        });
    }

    it("구독자 정보로 에피소드 업데이트 푸시 발송 메시지를 구성하는데 성공한다", async () => {
        process.env.USER_SETTINGS_HOOK = "http://notification/settings";
        process.env.LOCALE = "kor";
        const connection = await getConnection();
        sinon.stub(dbModule, "getConnection").returns(connection);
        const rows = await lambda.getSubscriptionUsers(66, 0);
        const messageBody = await lambda.makeMessagingBody(JSON.parse(event.Records[0].body), rows);
        expect(messageBody.type).to.equal("EPISODE_UPDATE");
        expect(messageBody.topic).to.equal("66");
        expect(messageBody.to.ids).to.have.length(5);
        expect(messageBody.to.ids).to.includes("koru56d69b530de080", "koru56d79b530de081", "koru4440fb9284edb9", "koru386bc18f406996", "kru22abf9acf77da2");
        expect(messageBody.hooks.before).to.have.length(1);
        expect(messageBody.hooks.before[0].type).to.equal("http");
        expect(messageBody.hooks.before[0].url).to.equal(process.env.USER_SETTINGS_HOOK);
        expect(messageBody.hooks.before[0].data.pushType).to.equal("EPISODE_UPDATE");
        expect(messageBody.hooks.before[0].data.region).to.equal(process.env.LOCALE);
        expect(messageBody.message.type).to.equal("EPISODE_UPDATE");
        expect(messageBody.message.title).to.equal("사랑해요 업데이트");
        expect(messageBody.message.body).to.equal("'어게인 1화 ~ 포지션5화'가 업데이트 되었습니다.");
        expect(messageBody.message.thumbnail).to.equal("https://${CDN_HOST}/C/46/c1/2x/02fab9c9-c7dd-454b-8731-cd0be951ee64");
        expect(messageBody.message.link).to.equal("kakaowebtoon://content/seo_99999/66");
    });
});
