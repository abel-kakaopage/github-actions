'use strict';
const AWS = require('aws-sdk');
const moment = require('moment');

AWS.config.update({
    region: process.env.AWS_REGION
});

const ALREADY_READ_DYNAMODB_TABLE = process.env.ALREADY_READ_DYNAMODB_TABLE;
const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

module.exports.main = (event, context, callback) => {
    console.log(JSON.stringify(event));
    const now = moment().utc().utcOffset(process.env.TIMEZONE_OFFSET);
    const ttl = now.endOf('day').unix();

    for (const key in event.records) {
        let batch = event.records[key].map((data) => {
            const msg = Buffer.from(data.value, 'base64').toString();
            console.info('Message: ', msg);

            const record = JSON.parse(msg);
            const event = record.event;

            if (event && event.type === 'CREATE') {
                const attributes = event.attributes;
                if (attributes && attributes.length === 1) {
                    const episodeRead = attributes[0]
                    if (episodeRead.userId) {
                        return {
                            PutRequest: {
                                Item: {
                                    user_id: {
                                        S: episodeRead.userId
                                    },
                                    expires_at: {
                                        N: String(ttl)
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }).filter(r => r != null);

        const processItemsCallback = function (err, data) {
            if (err) {
                console.error(err);
                callback(err);
            } else {
                console.info('DynamoDB response: ', data);
                if (data.UnprocessedItems[ALREADY_READ_DYNAMODB_TABLE]) {
                    dynamodb.batchWriteItem({RequestItems: data.UnprocessedItems}, processItemsCallback);
                }
            }
        };

        const ids = batch.map((param) => param.PutRequest.Item.user_id.S);
        console.info('ids: ', JSON.stringify(ids));

        batch = batch.filter((param, index) => ids.indexOf(param.PutRequest.Item.user_id.S) === index);
        console.info('Batch: ', JSON.stringify(batch));
        if (batch.length > 0) {
            const requestItems = {};
            requestItems[ALREADY_READ_DYNAMODB_TABLE] = batch;
            dynamodb.batchWriteItem({RequestItems: requestItems}, processItemsCallback);
        }
    }
};




