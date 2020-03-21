const createRepository = require('../storage/LocalStorage');
const { 
    sleep,
    createLog,
    getExponentialBackoff, 
} = require('../../utils')

const diretory = 'app/local-storage';

const defaultObject = {
    maxRetries:5,
    startTime: 15,
    dir:diretory,
    bufferMaxLimit: 3000
}

module.exports = class RetrySNS {
    constructor({
        maxRetries=5,
        bufferMaxLimit=3000,
        startTime=15,
        logging,
        dir=diretory,
        sns
    } = defaultObject) {
        this.sns = sns
        this.maxRetries = maxRetries;
        this.retryCounter = 0;
        this.log = createLog(logging)
        this.timer = null;
        this.repository = createRepository({ 
            bufferMaxLimit,
            logging, 
            dir
        })
        this.handleRetryStrategy = getExponentialBackoff.bind(null, startTime);
    }

    async saveError(data) {
        if(this.timer) {
            clearTimeout(this.timer)
        }
        this.timer = this.retryScheduler();
        await this.repository.save(data);
    }

    async retry(val) {
        // take a snapshot of messages to retry
       const pullFromSnapShot = await this.repository.pull();
       for await (const { key, messages, done } of pullFromSnapShot()) {
            if(done) {
                break;
            }
            const map = new Map(Object.entries(messages));
            try {
                for await(const [keyMap,data] of map.entries()) {
                    //console.log(`Retrying to publish the message: ${JSON.stringify(data,null,2)}`)
                    await this.sns.publish(data).promise();
                    map.delete(keyMap);
                    this.log(`Key ${keyMap} from Pack: ${key} was removed`)
                    await sleep(0.06)
                }
                await this.repository.clean(key)
            } catch (error) {
                this.log(`Erro: ${error.message}`)
                await this.repository.updateOnFail(
                    key, 
                    [...map.values()],
                    map.size !== messages.length
                );
                this.time = this.retryScheduler(++this.retryCounter);
                break;
            }
       }
    }

    retryScheduler(attempt=0) {
        if(this.maxRetries >= attempt) {
            return global.setTimeout(
                this.retry.bind(this), 
                this.handleRetryStrategy(attempt)
            );
        }
        this.retryCounter = 0;
    }  
}