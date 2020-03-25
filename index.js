require('./pollyfill');

const SNS = require("./Services/notifications/SNS");
const SQS = require("./Services/notifications/SQS");
const Redis = require("./Services/cache/Redis");
const S3 = require("./Services/storage/S3");
const Dynamo = require("./Services/DataBase/DynamoDB");
const KMS = require("./Services/secret/Kms");

const {
  getIndex,
  putIndex,
  getAllIndex,
  deleteIndex,
  putMappingType,
  searchTypeIndex,
  postData,
  getDataById
} = require("./Services/search/ElasticSearch");

exports.SNS_Post = function(snsURL, payload, subject, region, callback) {
  new SNS(region).post(snsURL, payload, subject)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.snsPostPromise = 
  async (snsURL, payload, subject, region) => 
    new SNS(region).post(snsURL, payload, subject)
    
exports.SNS_ListSubscriptionByTopic = function(snsURL, region, callback){
  new SNS(region).listSubscriptionsByTopic(snsURL, function(err, data){
    callback(err, data);
    });
};

exports.SQS_Get = function(queueURL, region, callback) {
  new SQS(region).get(queueURL, function(err, data) {
    callback(err, data);
  });
};

exports.SQS_Delete = function(queueURL, receiptHandle, region, callback) {
  new SQS(region).delete(queueURL, receiptHandle, function(err, data) {
    callback(err, data);
  });
};

exports.SQS_DeleteWithMonitor = function(
  queueURL,
  receiptHandle,
  arn,
  messageId,
  subject,
  region,
  callback
) {
  new SQS(region).deleteWithMonitor(
    queueURL,
    receiptHandle,
    arn,
    messageId,
    subject,
    function(err, data) {
      callback(err, data);
    }
  );
};

exports.SQS_ListQueues = function(queueNamePrefix, region, callback) {
  new SQS(region).listQueues(queueNamePrefix, function(err, data) {
    callback(err, data);
  });
};

exports.Redis_Get = function(key, host, port, callback) {
  new Redis(host, port).get(key, function(err, data) {
    callback(err, data);
  });
};

exports.Redis_Post = function(key, value, ttl, host, port, callback) {
  new Redis(host, port).post(key, value, ttl, function(err, data) {
    callback(err, data);
  });
};

exports.Redis_Delete = function(key, host, port, callback) {
  new Redis(host, port).delete(key, function(err, data) {
    callback(err, data);
  });
};

exports.S3_Get = function(bucket, key, callback) {
  new S3().get(bucket, key)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3GetPromise = async(bucket,key) => new S3().get(bucket,key)

exports.S3_Put = function(bucket, key, param, callback) {
  new S3().put(bucket, key, param)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3PutPromise = async(bucket, key, param) => new S3().put(bucket, key, param)

exports.S3_PutObject = function(param, callback) {
  new S3().putObject(param)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3PutObjectPromise = async(param) => new S3().putObject(param)

exports.S3_Upload = function(param, callback) {
  new S3().upload(param)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3UploadPromise = async(param) =>  new S3().upload(param);

exports.S3_DeleteObject = function (bucket, pathFileName, callback) {
  new S3().delete(bucket, pathFileName)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3DeleteObject = async(bucket, pathFileName) => new S3().delete(bucket, pathFileName)

exports.S3_ListObjects = function (bucket, directory, callback) {
  new S3().listObjects(bucket, directory)
    .then(data => callback(null, data))
    .catch(error => callback(error, null))
};

exports.s3ListObjects = async(bucket, directory) => new S3().listObjects(bucket, directory)

exports.Dynamo_Delete = function(object, region, callback) {
  new Dynamo(region).delete(object, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_Get = function(tableName, nameKey, key, region, callback) {
  new Dynamo(region).get(tableName, nameKey, key, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_Put = function(tableName, item, region, callback) {
  new Dynamo(region).put(tableName, item, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_getSortKey = function(tableName, keys, region, callback) {
  new Dynamo(region).getSortKey(tableName, keys, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_Query = function(object, region, callback) {
  new Dynamo(region).query(object, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_update = function(object, region, callback) {
  new Dynamo(region).update(object, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_scan = function(object, region, callback) {
  new Dynamo(region).scan(object, function(err, data) {
    callback(err, data);
  });
};

exports.Dynamo_batchWrite = function(object, region, callback) {
  new Dynamo(region).batchWrite(object, function(err, data) {
    callback(err, data);
  });
};

exports.Kms_encrypt = function(KeyId, region, Plaintext, callback) {
  new KMS(region).encrypt(KeyId, Plaintext, function(err, data) {
    callback(err, data);
  });
};

exports.Kms_decrypt = function(params, region, callback) {
  new KMS(region).decrypt(params, function(err, data) {
    callback(err, data);
  });
};


// ElasticSearch
exports.ES_getIndex = params => getIndex(params);
exports.ES_getAllIndex = params => getIndex(params);
exports.ES_putIndex = params => putIndex(params);
exports.ES_deleteIndex = params => deleteIndex(params);
exports.ES_putMappingType = params => putMappingType(params);
exports.ES_searchTypeIndex = params => searchTypeIndex(params);
exports.ES_postData = params => postData(params);
exports.ES_getAllIndex = params => getAllIndex(params);
exports.ES_getDataById = params => getDataById(params);
