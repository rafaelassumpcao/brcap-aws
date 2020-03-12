module.exports = {
    isInvalidInput(inputs=[]) {
        return inputs.some(condition => [null, undefined, ''].includes(condition));
    }
}