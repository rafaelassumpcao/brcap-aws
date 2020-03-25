const AWS = require('aws-sdk');

const RetrySNS = require('./RetrySNS');
const S3 = require('../storage/S3');
const { isInvalidInput, to } = require('../../utils');

const sns = new AWS.SNS({ 
    apiVersion: '2012-11-05', 
    httpOptions: { timeout: 25000 },
    region: 'sa-east-1',
    correctClockSkew: true,
})
const retry = new RetrySNS({ sns, logging: true });

const bucketQueueMonitor = "brasilcap-sns-history-notification";

module.exports = class SNS {

    constructor(region) {
        if(region && sns.config.region !== region)  {
            sns.config.update({ region });
        } 
        this.sns = sns;
    }

    async post(snsURL, payload, subject) {
        if(isInvalidInput([snsURL, payload, subject])) {
            return Promise.reject("Required sns post(snsUrl or Payload or subject) input is missing or in a invalid state.")
        }

        const now = new Date()
        const randomId = Math.floor(new Date().valueOf() + (Math.random() * Math.random()));
        payload.QueueMonitorId = randomId;
        const params = {
            "Message": JSON.stringify(payload),
            "MessageStructure": 'text',
            "TargetArn": snsURL,
            "Subject": subject,
        }
        const [error, data] =  await to(this.sns.publish(params).promise());

        if(error) {
            
            return retry.saveError(params)
        }

        const path = snsURL +"/"+now.getFullYear() +"-"+parseInt(now.getMonth()+1)+"-"+now.getDate()+"/"+now.getHours()+":"+now.getMinutes()+":"+now.getSeconds()+" - "

        await new S3().put(
            bucketQueueMonitor, 
            path+randomId.toString(), 
            payload,
        );
        console.log("BRCAP-AWS: dados gravados no S3.");
    }

    listSubscriptionsByTopic(snsURL, callback) {
        let params = {
            'TopicArn': snsURL
        };

        let items = [];

        this.sns.listSubscriptionsByTopic(params, function (err, listSubscriptionData) {
            if (err)
                console.log(err, err.stack);
            else
                listSubscriptionData.Subscriptions.forEach(function (element) {
                    if (element.Protocol == 'sqs') {
                        let item = {
                            'arn': element.Endpoint
                        };
                        items.push(item.arn);
                    }
                }, this);
            callback(err, items);
        });
    }
}
