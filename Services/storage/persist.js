const storage = require('node-persist');
const EventEmitter = require('')
const util = require('util');
const setTimeoutPromise = util.promisify(setTimeout);

module.exports = class RetrySNS  {
    constructor(config) {
        this.maxRetries = maxRetries;
        this.isOnPanic = true;
        this.repository = new Repository({bufferMaxLimit})
    }
  
   

    async saveError(data) {
        await this.repository.save(data);
        if(this.isOnPanic) {
            setTimeoutPromise(30000, {}).then(this.retry)
            this.isOnPanic = false;
        }
    }

    retry() {
       const { key, messages } =  await this.repository.pull();
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
    constructor({bufferMaxLimit=500}) {
        this.inMemoryData = [];
        this.storage = null;
        this.bufferMaxLimit = bufferMaxLimit;
        this.key = 1n;
        this.keysFileSystem = []

    }

    async createFileSystem(config){
        this.storage = await storage.create(config);
        return this;
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