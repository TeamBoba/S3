import config from '../../Config';
import async from 'async';
import bufferIndexOf from 'buffer-indexof';
import stream from 'stream';

const maxSize = 1048576;
const prependMaster = "part|";

function _parseChunk(chunk) {
    var bufsplit = require('buffer-split');
    var keysArray = bufsplit(chunk, new Buffer('|'));
    return keysArray;
}

function _splitData(totalValue, size) {
    const n_partitions = size / maxSize;
    const dataArray = [];

    for (let i = 0; i < n_partitions; i++) {
        dataArray.push(totalValue.slice(maxSize * i, maxSize * (i + 1)))
    }
    return dataArray;
}

function splitBuffer(buf,splitBuf,includeDelim){
    var search = -1
    , lines = []
    , move = includeDelim?splitBuf.length:0
    ;

    while((search = bufferIndexOf(buf,splitBuf)) > -1){
        lines.push(buf.slice(0,search+move));
        buf = buf.slice(search+splitBuf.length,buf.length);
    }

    lines.push(buf);
    return lines;
}

function joinBuffers(buffers, delimiter = ' ') {
    const d = Buffer.from(delimiter);

    return buffers.reduce((prev, b) => Buffer.concat([prev, d, b]));
}

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        const value = [];
         // spin up a Kinetic class object
        const testKinetic = config.kinetic.instance;
        request.on('data', data => {       // get data from request
            value.push(data);
        }).on('end', err => {
            if (size > maxSize) {
                if (err) {
                    return callback(err);
                }
                const totalValue = Buffer.concat(value);
                const keyTab = [];
                const dataArray = _splitData(totalValue, size);

                async.eachSeries(dataArray, (valuetoput, cb) => {
                    testKinetic.put(0, valuetoput, {}, (err, key) => {
                        keyTab.push(key);
                        cb(err);
                    });
                }, err => {
                    const masterKey = joinBuffers(keyTab, '|');
                    testKinetic.put(0, masterKey, {}, callback);
                });
            } else {
                if (err) {
                    return callback(err);
                }
                const obj = Buffer.concat(value);
                return testKinetic.put(0, obj, {}, callback);
            }
            return undefined;
        });
    },

    // should a new client be created to receive part gets? or is it ok to
    // recursively call get inside itself?
    get: function getK(keyValue, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const maxSize = 1048576;

        // check that part is isMaster
        let isMaster;

        const key = Buffer.from(keyValue);
        if (key.slice(0,5).compare(Buffer.from(prependMaster)) === 0) {
            let partSum;
            let masterValue = Buffer.allocUnsafe(0);
            // get the response from get request as stream; do to stream:
            return testKinetic.get(0, key, range, (err, stream) => {
                stream.on("data", (chunk) => {
                    masterValue = Buffer.concat([masterValue, chunk]);
                }).on('end', err => {
                    // get keys for each part
                    const keysArray = splitBuffer(masterValue, Buffer.from('|'));
                    // get value for each part and aggregate
                    var valueTab = [];
                    async.eachSeries(keysArray, (key, cb) => {
                        let partValue = Buffer.allocUnsafe(0);
                        testKinetic.get(0, Buffer.from(key), range, (err, stream) => {
                            // get partValue from stream, push to dataArray
                            stream.on("data", (chunk) => {
                                partValue = Buffer.concat([partValue, chunk]);
                            }).on('end', err => {
                                valueTab.push(partValue);
                                cb(err);
                            });
                        });
                    }, err => {
                        if (err) {
                            return callback(err);
                        }

                        const finalValue = Buffer.concat(valueTab);
                        // put partSum into response stream D:
                        var streamReadable = require('stream').Readable;
                        const res = new streamReadable({
                            read() {
                                this.push(finalValue);
                                this.push(null);
                            },
                        });

                        return callback(err, res)
                    });
                });
            });
        } else {
            return testKinetic.get(0, key, range, callback);
        }
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        const prepend = Buffer.from(prependMaster)

        if (key.slice(0, 5).toString() === prependMaster) {
            // do code for deleting master

            let masterKey = Buffer.allocUnsafe(0);
            testKinetic.get(0, key.slice(5), undefined, (err, stream) => {
                stream.on('data', chunk => {
                    masterKey = Buffer.concat([masterKey, chunk])
                }).on('end', err => {
                    if (err) {
                        return callback(err);
                    }
                    const keyTab = splitBuffer(masterKey, Buffer.from("|"));

                    async.eachSeries(keyTab, (key, cb) => {
                        testKinetic.delete(0, key, {}, err => {
                            cb(err);
                        });
                    }, (err) => {
                        testKinetic.delete(0, key.slice(5), {}, err => {
                            return callback(err);
                        });
                    });
                });
            });

        } else {
            return testKinetic.delete(0, key, {}, callback);
        }

        return undefined;
    },
};

export default backend;
