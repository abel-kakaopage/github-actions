'use strict';
const AWS = require("aws-sdk");
AWS.config.update({
    region: process.env.REGION
})
const sqs = new AWS.SQS();
const {uid} = require('uid');
const kms = new AWS.KMS();
var i18n = new (require('i18n-2'))({
    locales: ['en', 'ko', 'id', 'th', 'zht', 'fr'] // default locale english
});
const dbModule = require('./db-module');

function getSubscriptionUsers(contentId, lastId) {
    const SIZE = 30;
    return new Promise(async (resolve, reject) => {
        const connection = await dbModule.getConnection();
        const sql = `            
            SELECT id, user_id 
            FROM popularity.subscription IGNORE INDEX (PRIMARY) 
            WHERE content_id= ?
            AND id > ?
            AND notification_enabled_flag=true
            ORDER BY id ASC
            LIMIT ?`;
        const values = [contentId, lastId, SIZE];
        connection.query(sql, values, function (err, result) {
            if (result)
                resolve(result);
            if (err) {
                console.error("[Error] getSubscriptionUsers", err);
                reject(err);
            }
        });
        connection.end(function (err) {
        });
    });
}

function makeMessagingBody(message, rows) {
    let userIds = [];
    for (let i = 0; i < rows.length; i++) {
        userIds.push(rows[i].user_id);
    }

    const userSettingsHook = {
        type: "http",
        url: process.env.USER_SETTINGS_HOOK,
        data: {
            pushType: 'EPISODE_UPDATE',
            region: process.env.LOCALE
        }
    }

    const deviceSettingsHook = {
        type: "http",
        url: process.env.DEVICE_SETTINGS_HOOK,
        data: {
            marketingPush: false,
            region: process.env.LOCALE
        }
    }

    let beforeHook = [];
    beforeHook.push(userSettingsHook);
    let readyHook = [];
    readyHook.push(deviceSettingsHook);
    let hooks = {};
    hooks.before = beforeHook;
    // hooks.ready = readyHook;

    // push message
    i18n.setLocale(message.language);
    let pushBody = "";
    if (message.last_title) {
        pushBody = i18n.__('push.multi-message', message.title, message.last_title);
    } else {
        pushBody = i18n.__('push.single-message', message.title);
    }
    const CDN_HOST = 'https://${CDN_HOST}/';
    const pushMessage = {
        type: "EPISODE_UPDATE",
        title: i18n.__('push.title', message.content_title),
        body: pushBody,
        thumbnail: CDN_HOST + message.asset_thumbnail_image,
        link: `kakaowebtoon://content/${message.seo_id}/${message.content_id}`
    };

    const uuid = uid(32);
    const messageBody = {
        id: uuid,
        type: "EPISODE_UPDATE",
        topic: `${message.content_id}`,
        to: {ids: userIds},
        hooks: hooks,
        message: pushMessage
    };
    // console.info(`[Message] - ${JSON.stringify(messageBody, null, 4)}`);
    return messageBody;
}

async function sendSQS(message, queueUrl) {
    const params = {
        MessageBody: JSON.stringify(message),
        QueueUrl: queueUrl,
    };
    await sqs.sendMessage(params).promise();
}

module.exports.main = async event => {
    try {
        if (event) {
            console.info("[event - raw data]");
            console.info(JSON.stringify(event));
            const records = event.Records;
            const MAX_SEND_COUNT = 50000;
            let sendTotal = 0;
            if (records && records.length > 0) {
                for (const record of records) {
                    let message = JSON.parse(record.body);
                    let loop = true;
                    let lastId = message.last_id | 0;
                    console.info("Initial lastId: " + lastId);
                    while (loop) {
                        const rows = await getSubscriptionUsers(message.content_id, lastId);
                        if (rows.length > 0) {
                            await sendSQS(makeMessagingBody(message, rows), process.env.QUEUE_URL);
                            lastId = rows[rows.length - 1].id;
                            sendTotal += rows.length;
                            console.info(`ContentId: ${message.content_id}, sendTotal: ${sendTotal},Title: ${message.content_title}, Sended: ${rows.length}, lastId: ${lastId}`);
                        } else {
                            console.info(`There is no more content subscriber => ContentId: ${message.content_id}, Title: ${message.content_title}`);
                            loop = false;
                        }
                        // 최대 발송수를 초과 + 전송할 데이터가 남은 경우 : 작업을 종료하고 다음 job에 이어서 처리되도록 lastId 정보를 실어 SQS에 저장.
                        if (loop && sendTotal >= MAX_SEND_COUNT) {
                            loop = false;
                            message.last_id = lastId;
                            await sendSQS(message, process.env.MESSAGE_QUEUE_URL);
                            console.info(`Exceed Max Send Count. Continue next job: ${JSON.stringify(message)}`);
                        }
                    }
                }
            }
        } else {
            console.info("There is no content update message");
        }
    } catch (err) {
        console.error('[ERROR]', err);
    }
}

module.exports = {
    getSubscriptionUsers,
    makeMessagingBody
}
