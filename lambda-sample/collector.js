const AWS = require("aws-sdk");
AWS.config.update({
    region: process.env.REGION
})
const {Kafka} = require('kafkajs')
const {v4: uuid4} = require("uuid");
const {version: uuidVersion} = require("uuid");
const {validate: uuidValidate} = require("uuid");

// Send Kafka Message
async function produceKafkaMessageBatch(messages) {
    try {
        const kafka = new Kafka({
            clientId: 'publish-tiara-event-log',
            brokers: process.env.KAFKA_BROKER.split(","),
            connectionTimeout: 2000
        })
        const producer = kafka.producer()
        await producer.connect()
        await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: messages
        })
        await producer.disconnect();
    } catch (err) {
        console.error('[ERROR] produceKafkaMessageBatch', err);
        return new Error(err);
    }
}

function makeEventMessage(message) {
    let hits = message.hit;
    for (let i = 0; i < hits.length; i++) {
        // origin_request_timestamp 추가
        if (hits[i].custom) {
            hits[i].custom["origin_request_timestamp"] = message.collected_at;
        } else {
            hits[i].custom = {"origin_request_timestamp": message.collected_at};
        }
        // WEB일 경우 cookie정보 추가
        if (hits[i].sdk && hits[i].sdk.type === "WEB") {
            if (message.cookie) {
                if (hits[i].user) {
                    hits[i].user["tuid"] = message.cookie.tuidP;
                    hits[i].user["tsid"] = message.cookie.tsidP;
                    hits[i].user["uuid"] = message.cookie.uuidP;
                    hits[i].user["suid"] = message.cookie.suidP;
                } else {
                    hits[i].user = {
                        "tuid": message.cookie.tuidP,
                        "tsid": message.cookie.tsidP,
                        "uuid": message.cookie.uuidP,
                        "suid": message.cookie.suidP
                    };
                }
            }
        }
        // env에 clientIP 추가
        if (hits[i].env) {
            hits[i].env["ip"] = message.ip;
        } else {
            hits[i].env = {"ip": message.ip};
        }
        // auth_token 삭제
        if (hits[i].user_ex_account) {
            delete hits[i].user_ex_account["auth_token"];
        }
        // etc 삭제
        if (hits[i].etc) {
            delete hits[i].etc;
        }
    }
}

function makePartitionKey(message) {
    let hits = message.hit;
    let partitionKey;
    for (let i = 0; i < hits.length; i++) {
        if (hits[i].user_ex_account) {
            if (!partitionKey && hits[i].user_ex_account.user_id)
                partitionKey = hits[i].user_ex_account.user_id;
        }
    }
    if (!partitionKey) {
        partitionKey = uuid4();
    }
    return partitionKey;
}

function uuidValidateV4(uuid) {
    return uuidValidate(uuid) && uuidVersion(uuid) === 4;
}

module.exports = {
    produceKafkaMessageBatch,
    makeEventMessage,
    makePartitionKey,
    uuidValidateV4
}
