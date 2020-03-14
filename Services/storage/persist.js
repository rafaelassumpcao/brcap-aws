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
        maxRetries,
        bufferMaxLimit,
        startTime,
        sns
    }={
        maxRetries:5,
    }) {
        this.sns = sns
        this.maxRetries = maxRetries;
        this.isOnPanic = true;
        this.repository = new Repository({ bufferMaxLimit, storage })
        this.handleRetryStrategy = constructExponentialBackoffStrategy(startTime);
    }


    async saveError(data) {
        await this.repository.save(data);
        if(this.isOnPanic) {
            this.isOnPanic = false;
            setTimeoutPromise(this.handleRetryStrategy(), {})
                .then(this.retry.bind(this))
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
            // TODO: terminar essa logica doida aqui
            try {
                for await(const data of messages) {
                    toUpdateOnFail = messages;
                    console.log(`Retrying to publish the message: ${JSON.stringify(data,null,2)}`)
                    await this.sns.puplish(data).promise();
                    messages.shift();
                    console.log(`Message Ok`);
                }
                this.repository.clean(key);
            } catch (error) {
                await this.repository.updateOnFail(key, messages)
            }
           
            // chunkify(messages,4, true)
            
       }
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
        log(`snapShot: ${JSON.stringify(snapShot, null, 2)}`)
        return async function*() {
            const { memory, fsKeys } = snapShot; 
            while(memory.length && fsKeys.length) {
                if(snapShot.memory.length) {
                    const data = { 
                        key: 0, 
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
        await storage.updateItem(key, messages);
    }

    clean(key) {
        await storage.removeItem(key);
        console.log(`Key: ${key} was removed`);
    }
}

Repository.createFileSystem({
    dir: 'app/local-storage/',
    logging: true,
})