// Load the AWS SDK for Node.js
const AWS = require('aws-sdk');
const storage = require('node-persist');

storage.init({
    dir: 'brcap/sns',
    logging: true
})
.then(() => console.log('Storage criado com sucesso'))
var BRCAPAWS = require('../../index.js');


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

        // if (payload === undefined || payload === null || payload === '') {
        //     Promise.reject("payload missing or in a invalid state.", null);
        // } else if (snsURL === undefined || snsURL === null || snsURL === '') {
        //     Promise.reject("snsURL missing or in a invalid state.", null);
        // } else if (subject === undefined || subject === null || subject === '') {
        //     Promise.reject("subject missing or in a invalid state.", null);
        // } else {

            const now = new Date()
            const randomId = Math.floor(new Date().valueOf() + (Math.random() * Math.random()));

            console.log(Date.now().toString(36))

            payload.QueueMonitorId = randomId;
           const data =  await this.sns.publish({
                Message: JSON.stringify(payload),
                MessageStructure: 'text',
                TargetArn: snsURL,
                Subject: subject,
            }).promise();

            const path = snsURL +"/"+now.getFullYear() +"-"+parseInt(now.getMonth()+1)+"-"+now.getDate()+"/"+now.getHours()+":"+now.getMinutes()+":"+now.getSeconds()+" - "

                    BRCAPAWS.S3_Put(bucketQueueMonitor, path+randomId.toString(), payload, function (err, s3Data) {
                        if (err) {
                            console.log(err);
                        } else {
                            console.log("BRCAP-AWS: dados gravados no S3.");
                        }
                    });
                }

        }
    }
}
