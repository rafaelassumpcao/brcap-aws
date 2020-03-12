// Load the AWS SDK for Node.js
const AWS = require('aws-sdk');
const { isInvalidInput } = require('../../utils');

const s3 = new AWS.S3({apiVersion: '2006-03-01'});

module.exports = class S3 {

    constructor() {
        this.s3 = s3;
    }

    get(bucket, key) {
        if(isInvalidInput([bucket, key])) {
            return Promise.reject("bucket or key missing or in a invalid state.")
        }

        return this.s3.getObject({
            Bucket: bucket,
            Key: key
        }).promise();
    }

    put(bucket, key, param) {
        const params = {
            Bucket: bucket,
            Key: key,
            Body: JSON.stringify(param)
        };
       return this.s3.putObject(params).promise();
    }

    putObject(params) {
        return this.s3.putObject(params).promise();
    }

    upload(params) {
        return this.s3.upload(params).promise();
    }

    delete(bucket, pathFileName) {
        const params = {
            Bucket: bucket,
            Key: pathFileName,
        };
        return this.s3.deleteObject(params).promise();
    }

    listObjects(bucket, directory) {
       return this.s3.listObjectsV2({
            Bucket: bucket,
            Prefix: directory,
        }).promise();
    }
}