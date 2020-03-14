module.exports = {
    isInvalidInput(inputs=[]) {
        return inputs.some(condition => [null, undefined, ''].includes(condition));
    },
    constructExponentialBackoffStrategy(startTime=15) {
        let attempts = -1;
        return () => {
          attempts += 1;
          return getExponentialBackoff(startTime, attempts);
        };
    },
    sleep(timeInSeconds) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve();
            }, timeInSeconds*1000)
        })
    },
    chunkify(a, n, balanced) { 
        /*  metodo extraido de https://stackoverflow.com/a/8189268 */
        if (n < 2)
            return [a];
        var len = a.length,
                out = [],
                i = 0,
                size;
        if (len % n === 0) {
            size = Math.floor(len / n);
            while (i < len) {
                out.push(a.slice(i, i += size));
            }
        }
        else if (balanced) {
            while (i < len) {
                size = Math.ceil((len - i) / n--);
                out.push(a.slice(i, i += size));
            }
        }
        else {
            n--;
            size = Math.floor(len / n);
            if (len % size === 0)
                size--;
            while (i < size * n) {
                out.push(a.slice(i, i += size));
            }
            out.push(a.slice(size * n));
        }
        return out;
    }
}

function getExponentialBackoff(startTimeInSeconds,attempts) {
    return Math.pow(2, attempts) * (startTimeInSeconds * 1000);
}