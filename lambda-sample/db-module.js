const mysql = require("mysql");
const AWS = require("aws-sdk");
const kms = new AWS.KMS();

module.exports = {
    decryptData: function(buffer) {
        const params = {
            CiphertextBlob: Buffer.from(buffer, "base64")
        }
        return new Promise((resolve, reject) => {
            kms.decrypt(params, (err, data) => {
                if (err) reject(err);
                else resolve(data.Plaintext);
            })
        })
    },
    getConnection: async function() {
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
}
