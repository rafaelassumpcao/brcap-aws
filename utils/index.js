module.exports = {
    isInvalidInput(inputs=[]) {
        return inputs.some(condition => [null, undefined, ''].includes(condition));
    },
    sleep(timeInSeconds) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve();
            }, timeInSeconds*1000)
        })
    },
    getExponentialBackoff(startTimeInSeconds,attempts) {
        return Math.pow(2, attempts) * (startTimeInSeconds * 1000);
    },
    createLog(logging=false) {
        return logging 
            ? (logMessage) => console.log(logMessage)
            : _ => _
    },
    to(promise) {
        return promise
                .then(data => [null, data])
                .catch(error => [error])
    }
}