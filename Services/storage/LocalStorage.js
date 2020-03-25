const storage = require('node-persist');
const { v4: uuidv4 } = require('uuid');

const { sleep, createLog } = require('../../utils')

class Repository {
    constructor(config) {
        this.inMemoryData = new Map();
        this.bufferMaxLimit = config.bufferMaxLimit;
        this.key = 1n;
    }

    createStorage(config) {
        storage.init(config)
            .then(result => {
                console.log(`[brcap-aws] Local storage created at \x1b[32m${result.dir}\x1b[0m`)
            })
            .catch(error => 
                console.log(`No local storage was created duo to some error: ${error.message}`)
            )
        return this;
    }

    async save(data) {
        this.inMemoryData = (data instanceof Map) 
            ? new Map([...this.inMemoryData, ...data])
            : this.inMemoryData.set(uuidv4(),data);
        
        if(this.inMemoryData.size > this.bufferMaxLimit) {
            await this.pushToFileSystem();
        }
    }

    async pushToFileSystem() {
        await storage.setItem(`${this.key}`, Object.fromEntries([...this.inMemoryData]));
        this.inMemoryData.clear();
        this.key++;
    }

    async *pull() {
        const fsKeys = await storage.keys();
        while(this.inMemoryData.size || fsKeys.length) {
            // pull out from memory (Buffer)
            if(this.inMemoryData.size) {
                const data = { 
                    key: "0", // position 0 reserved for memory access
                    messages: [...this.inMemoryData], 
                    done:false 
                };
                this.inMemoryData.clear();
                yield data;
                continue;
            }
            // pull out from filesystem
            const filesystemMessages = await storage.getItem(fsKeys[0]);
            const fsData =  {
                key: fsKeys[0],
                messages: Object.entries(filesystemMessages),
                done:false
            }
            await storage.removeItem(fsKeys[0]);
            fsKeys.shift()
            yield fsData;
        }
        yield { done: true };
    }

    async updateOnFail(key, messagesMap, isNecessaryIO=true) {
        if(key === '0') {
            return this.inMemoryData = new Map([...this.inMemoryData, ...messagesMap])
        }
        if(isNecessaryIO) {
           await storage.updateItem(key, Object.fromEntries([...messagesMap]));
        }
    }

     async clean(key, map) {
        if(key === '0') return;
        await storage.removeItem(key);
    }
}

module.exports = (config) =>  new Repository(config).createStorage(config);