const storage = require('node-persist');

const { sleep, createLog } = require('../../utils')

class Repository {
    constructor(config) {
        this.inMemoryData = [];
        this.bufferMaxLimit = config.bufferMaxLimit;
        this.key = 1n;
        this.log = createLog(true);
        this.storage = storage.create(config);
    }

    createStorage() {
        this.storage.init()
            .then(result => {
                console.log(`[brcap-aws] Local storage created at \x1b[32m${result.dir}\x1b[0m`)
            })
            .catch(error => 
                console.log(`No local storage was created duo to some error: ${error.message}`)
            )
        return this;
    }

    async save(data) {
        await sleep(0.12)
        this.inMemoryData.push(data);
        if(this.inMemoryData.length >= this.bufferMaxLimit) {
            this.pushToFileSystem();
        }
    }

    async pushToFileSystem() {
        const bufferCopy = [...this.inMemoryData];
        this.inMemoryData=[];
        await this.storage.setItem(`${this.key}`, bufferCopy);
        this.key++;
    }

    async pull() {
        // create a snapshot from memory and file system state
        const snapShot = {
            memory:[...this.inMemoryData.splice(-this.bufferMaxLimit)],
            fsKeys: await this.storage.keys()
        }
        // each call pulls out one block of message
        return async function*() {
            while(snapShot.memory.length || snapShot.fsKeys.length) {
                // pull out from memory (Buffer)
                if(snapShot.memory.length) {
                    const data = { 
                        key: "0", // position 0 reserved for memory access
                        messages: [...snapShot.memory], 
                        done:false 
                    };
                    snapShot.memory=[];
                    yield data;
                    continue;
                }
                // pull out from filesystem
                const fsData =  {
                    key: snapShot.fsKeys[0],
                    messages: await this.storage.getItem(snapShot.fsKeys[0]),
                    done:false
                }
                snapShot.fsKeys.shift()
                yield fsData;
            }
            yield { done: true };
        }
        
    }

    async updateOnFail(key, messages, isNecessaryIO=true) {
        this.log(`Updating on Fail - key: ${key}`)
        if(key === '0') {
            return this.inMemoryData.push(...messages)
        }
        if(isNecessaryIO) {
            await this.storage.updateItem(key, messages);
        }
    }

    async clean(key) {
        this.log(`Cleaning key: ${key} (${key === '0' ? 'Memory':'Filesystem'})`)
        if(key === '0') return;
        await this.storage.removeItem(key);
        this.log(`Key: ${key} was removed`);
    }
}

module.exports = (config) =>  new Repository(config).createStorage();