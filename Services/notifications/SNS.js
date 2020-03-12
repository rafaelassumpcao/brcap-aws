// Load the AWS SDK for Node.js
const AWS = require('aws-sdk');
const storage = require('node-persist');
const S3 = require('../storage/S3');
const RetrySNS = require('../storage/persist');
const retry = new RetrySNS();
//const s3 = new S3();
const { isInvalidInput } = require('../../utils');


const sns = new AWS.SNS({ 
    apiVersion: '2012-11-05', 
    httpOptions: { timeout: 25000 },
    region: 'sa-east-1',
    correctClockSkew: true,
})

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
            Message: JSON.stringify(payload),
            MessageStructure: 'text',
            TargetArn: snsURL,
            Subject: subject,
        }
        try {
                const data =  await this.sns.publish().promise();
    
            const path = snsURL +"/"+now.getFullYear() +"-"+parseInt(now.getMonth()+1)+"-"+now.getDate()+"/"+now.getHours()+":"+now.getMinutes()+":"+now.getSeconds()+" - "

            await new S3().put(
                bucketQueueMonitor, 
                path+randomId.toString(), 
                payload,
            );
            console.log("BRCAP-AWS: dados gravados no S3.");
        } catch (error) {
            retry.saveError();
        }
        
    }
}
