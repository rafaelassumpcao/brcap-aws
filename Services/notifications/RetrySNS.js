const createRepository = require('../storage/LocalStorage');
const {
    to,
    sleep,
    createLog,
    getExponentialBackoff, 
} = require('../../utils')

const diretory = 'app/local-storage';
const info = console.log.bind(null, '[brcap-aws] - ')

const defaultObject = {
    maxRetries:5,
    startTime: 15,
    dir:diretory,
    bufferMaxLimit: 3000
}

module.exports = class RetrySNS {
    constructor({
        maxRetries=5,
        bufferMaxLimit=1500,
        startTime=15,
        logging,
        dir=diretory,
        sns
    } = defaultObject) {
        this.sns = sns
        this.maxRetries = maxRetries;
        this.retryCounter = 0;
        this.timer = null;
        this.repository = createRepository({ 
            bufferMaxLimit,
            logging, 
            dir
        })
        this.handleRetryStrategy = getExponentialBackoff.bind(null, startTime);
    }

   async saveError(data,opts={attempt: 0}) {
        info(
            `Notificao de sns encontrou um erro. Reenvio em ${this.startTime}s`
        );
        clearTimeout(this.timer)
        this.timer = this.retryScheduler(opts.attempt);
        await this.repository.save(data);
    }

    async retry() {       
       for await (const { key, messages, done } of this.repository.pull()) {
            if(done) {
                info('Mensagens reenviadas')
                break;
            }
            const map = new Map(messages);
            try {
                for await(const [keyMap,data] of map.entries()) {
                    await this.sns.publish(data).promise();
                    map.delete(keyMap);
                }
                await this.repository.clean(key, map)
            } catch (error) {
                info('Ocorreu um erro durante reenvio. Tentando novamente')
                await this.saveError(map, { attempt: ++this.retryCounter })
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