const storage = require('node-persist');
const util = require('util');
const { 
    constructExponentialBackoffStrategy, 
    sleep,
    chunkify,
} = require('../../utils')
const setTimeoutPromise = util.promisify(setTimeout);
const log = console.log

module.exports = class RetrySNS {
    constructor({
        maxRetries=5,
        bufferMaxLimit,
        startTime,
        sns
    }={
        maxRetries:5,
    }) {
        this.sns = sns
        this.maxRetries = maxRetries;
        this.retryCounter = maxRetries;
        this.startTime = startTime;
        this.isOnPanic = true;
        this.repository = new Repository({ bufferMaxLimit, storage })
        this.handleRetryStrategy = constructExponentialBackoffStrategy(startTime);
    }


    async saveError(data) {
        await this.repository.save(data);
        if(this.isOnPanic) {
            this.isOnPanic = false;
            this.retryScheduler();
        }
    }

    async retry(val) {
        // take a snapshot of messages to retry
       const pullFromSnapShot = await this.repository.pull();
       for await (const { key, messages, done } of pullFromSnapShot()) {
           
            if(done) {
                this.isOnPanic = true;
                break;
            }
            let toUpdateOnFail;

            try {
                for await(const data of messages) {
                    toUpdateOnFail = messages;
                    console.log(`Retrying to publish the message: ${JSON.stringify(data,null,2)}`)
                    await this.sns.publish(data).promise();
                    messages.shift();
                    console.log(`Message Ok`);
                }
                await this.repository.clean(key)
                ean(key);
            } catch (error) {
                if(isInMemoryData) {

                }
                await this.repository.updateOnFail(key, toUpdateOnFail);
                this.isOnPanic = true;
                this.retryScheduler();
            }
           
            // chunkify(messages,4, true)
            
       }
    }

    async retryScheduler() {
        this.retryCounter--;
        if(this.retryCounter >= 0) {
            setTimeoutPromise(this.handleRetryStrategy(), {})
                    .then(this.retry.bind(this))
            return false;
        }
        this.retryCounter = this.maxRetries;
        this.handleRetryStrategy = constructExponentialBackoffStrategy(this.startTime);
        return true;
    }

    
}

class Repository {
    constructor({bufferMaxLimit=100 }) {
        this.inMemoryData = [];
        this.bufferMaxLimit = bufferMaxLimit;
        this.key = 1n;
        this.keysFileSystem = []

    }

    static createFileSystem(config){
        storage.init(config)
            .then(result => {
                log(`[brcap-aws] Local storage created at \x1b[32m${result.dir}\x1b[0m`)
            })
            .catch(error => 
                log(`No local storage was created duo to some error: ${error.message}`)
            )
    }

    async save(data) {
        await sleep(0.12)
        this.inMemoryData.push(data);
        if(this.inMemoryData.length >= this.bufferMaxLimit) {
            const bufferCopy = [...this.inMemoryData];
            this.inMemoryData=[];
            await storage.setItem(`${this.key}`, bufferCopy);
            this.keysFileSystem.push(`${this.key}`)
            this.key++;
        }
    }

    async pull() {
        const snapShot = {
            memory:[...this.inMemoryData.slice(-this.bufferMaxLimit)], // muta a referencia em memoria, mas nao guarda com o objeto
            fsKeys: [...this.keysFileSystem.slice(-100)] // garantir que vai pegar tudo do fs
        }
        // log(`snapShot: `)
        return async function*() {
            const { memory, fsKeys } = snapShot; 
            while(memory.length && fsKeys.length) {
                if(snapShot.memory.length) {
                    const data = { 
                        key: "0", 
                        messages: [...snapShot.memory], 
                        done:false 
                    };
                    snapShot.memory=[];
                    yield data;
                    continue;
                }
                const fsData =  {
                    key: fsKeys[0],
                    messages: await storage.getItem(fsKeys[0]),
                    done:false
                }
                fsKeys.shift()
                yield fsData;
            }
            yield { done: true }
        }
        
    }

    async updateOnFail(key, messages) {
        // is in memory data?
        if(key === '0') {
            return this.inMemoryData.push(...messages)
        }
        await storage.updateItem(key, messages);
    }

    async clean(key) {
        if(key === '0') return;
        await storage.removeItem(key);
        console.log(`Key: ${key} was removed`);
    }
}

Repository.createFileSystem({
    dir: 'app/local-storage/',
    logging: true,
})