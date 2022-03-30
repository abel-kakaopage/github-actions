const AWS = require("aws-sdk");
AWS.config.update({
    region: process.env.REGION
})
const mysql = require('mysql');
const kms = new AWS.KMS();
const sqs = new AWS.SQS();

function sendSQS(message) {
    const params = {
        MessageBody: JSON.stringify(message),
        QueueUrl: process.env.QUEUE_URL,
    };
    return sqs.sendMessage(params).promise();
}

function decryptData(buffer) {
    const params = {
        CiphertextBlob: Buffer.from(buffer, "base64")
    }
    return new Promise((resolve, reject) => {
        kms.decrypt(params, (err, data) => {
            if (err) reject(err);
            else resolve(data.Plaintext);
        })
    })
}

async function createConnection() {
    const dbUser = Buffer.from(await decryptData(process.env.DB_USER), 'base64');
    const dbPassword = Buffer.from(await decryptData(process.env.DB_PASSWD), 'base64');

    return mysql.createConnection({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: dbUser,
        password: dbPassword,
        database: process.env.DB_NAME
    });
}

function getMaxInvokeId() {
    return new Promise(async (resolve, reject) => {
        const connection = await createConnection();
        const sql = `SELECT min(id) as id
                     FROM tx_outbox
                     WHERE send_flag = false LIMIT 1`;
        connection.query(sql, function (err, result) {
            if (result)
                resolve(result);
            if (err) {
                console.error("[Error] getMaxInvokeId", err);
                reject(err);
            }
        });
        connection.end(function (err) {
        });
    });
}

function makeSqsMessage(rows) {
    if (rows.length === 1 && rows[0].id > 0) {
        const message = {"id": rows[0].id};
        console.info('outbox exists = ' + JSON.stringify(message));
        return message;
    } else {
        console.info('outbox not exist');
    }
}

async function checkAndInvoke() {
    const rows = await this.getMaxInvokeId();
    if (rows) {
        const message = this.makeSqsMessage(rows);
        if (message) {
            await this.sendSQS(message);
        }
        return message;
    }
}

module.exports = {
    getMaxInvokeId,
    checkAndInvoke,
    makeSqsMessage,
    sendSQS
}
