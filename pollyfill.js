const fromEntries = require('object.fromentries');

if (!Object.fromEntries) {
    fromEntries.shim();
}
