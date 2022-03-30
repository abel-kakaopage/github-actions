'use strict';
const AWS = require("aws-sdk");
const multipart = require("parse-multipart");
const multer = require("multer");

AWS.config.update({
  region: "ap-northeast-2"
})
const sqs = new AWS.SQS();

function sendSQS(message) {
  const params = {
    MessageBody: JSON.stringify(message),
    QueueUrl: "https://sqs.ap-northeast-2.amazonaws.com/593352196761/popularity-subscription-event-invoker-sandbox",
  };
  return sqs.sendMessage(params).promise();
}

module.exports.main = event => {
  let message = {"id": 1};
  sendSQS(message);
  // console.log(event);
  // const bodyBuffer = Buffer.from(event["body-json"].toString(), "base64");
  // const boundary = multipart.getBoundary(event.params.header["Content-Type"]);
  // const parts = multipart.Parse(bodyBuffer, boundary);
  // const files = getFiles(parts);
  // console.log(files);
};

const getFiles = function (parts) {
  const files = [];
  parts.forEach((part) => {

    const buffer = part.data
    const fileFullName = part.filename;

    const filefullPath = "각자 버킷 URL 적기" + fileFullName;

    const params = {
      Bucket: "S3 Bucket 이름",
      Key: fileFullName,
      Body: buffer,
    };

    const uploadFile = {
      size: buffer.toString("ascii").length,
      type: part.type,
      name: fileFullName,
      full_path: filefullPath,
    };

    files.push({ params, uploadFile });
  });
  return files;
};
