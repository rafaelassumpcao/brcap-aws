const storage = require('node-persist');
const util = require('util');
const setTimeoutPromise = util.promisify(setTimeout);

module.exports = class RetrySNS  {
    constructor({
        maxRetries=5,
        bufferMaxLimit,
        startTime,
        storage,
    }) {
        this.maxRetries = maxRetries;
        this.isOnPanic = true;
        this.repository = new Repository({ bufferMaxLimit, storage })
        this.handleRetryStrategy = this.constructExponentialBackoffStrategy(startTime);
    }


    async saveError(data) {
        await this.repository.save(data);
        if(this.isOnPanic) {
            setTimeoutPromise(30000, {}).then(this.retry)
            this.isOnPanic = false;
        }
    }

    async retry() {
       const { key, messages } =  await this.repository.pull();
       console.log(key);
    }

    getExponentialBackoff(startTimeInSeconds,attempts) {
        return Math.pow(2, attempts) * (startTimeInSeconds * 1000);
    }
       
    constructExponentialBackoffStrategy(startTime=15) {
        let attempts = -1;
        return () => {
          attempts += 1;
          return getExponentialBackoff(startTime, attempts);
        };
      }
}

class Repository {
    constructor({bufferMaxLimit=500, storage }) {
        this.inMemoryData = [];
        this.bufferMaxLimit = bufferMaxLimit;
        this.key = 1n;
        this.keysFileSystem = []

    }

    static createFileSystem(config){
        try {
            this.storage = storage.create(config);
            console.log(`Retryable storage created at ${storage}`)
        } catch (error) {
            console.log(`No local storage was created duo to some error: ${error.message}`)
        }  
    }

    async save(data) {
        this.inMemoryData.push(data);
        if(this.inMemoryData.length >= this.bufferMaxLimit) {
            await this.storage.setItem(key, this.inMemoryData);
            this.inMemoryData = [];
            this.keysFileSystem.push(key)
            key++;
        }
    }

    async pull() {
        if(this.inMemoryData.length) {
            const messages = [...this.inMemoryData];
            return { key: 0, messages };
        }
        return {
            key: this.keysFileSystem[0],
            messages: await storage.getItem(this.keysFileSystem[0])

        }
    }
}

Repository.createFileSystem({
    dir: 'brcap/sns',
    logging: true,
})