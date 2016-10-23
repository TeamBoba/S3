import config from '../../Config';
import async from 'async';

// function x (err, key) {
//     console.log(err)
//     console.log()
// }

const maxSize = 1048576;

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
	console.log('SIZE')
	console.log(totalValue.slice(maxSize * i, maxSize * (i + 1)).length)
    }
    console.log(dataArray);
    console.log("####################################################################################################");

    console.log("####################################################################################################");

    console.log("####################################################################################################");

    console.log("####################################################################################################");

    console.log("####################################################################################################");

    return dataArray;
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
        request.on('data', data => {		     // get data from request
            value.push(data);
        }).on('end', err => {
            if (size > maxSize) {
                if (err) {
                    return callback(err);
                }
                const totalValue = Buffer.concat(value);
                const keyTab = [];
                const dataArray = _splitData(totalValue, size);
                // dataArray.forEach(valuetoput, (err) => {
                //     testKinetic.put(valuetoput, {}, (err, key) => {
                // 	keyTab.push(key);
                //     })
                // })
                async.eachSeries(dataArray, (valuetoput, cb) => {
                    testKinetic.put(valuetoput, {}, (err, key) => {
                        keyTab.push(key);
                        cb(err);
                    });
                }, err => {
                    const masterKey = joinBuffers(keyTab, '|');
                    console.log('tohex', masterKey.toString('hex'));
                    testKinetic.put(masterKey, {}, callback);
                });
                // const masterKey = Buffer.concat(keyTab);
                // testKinetic.put(masterKey, {}, callback)
            } else {
                if (err) {
                    return callback(err);
                }
                const obj = Buffer.concat(value);
                return testKinetic.put(obj, {}, callback);
            }
            return undefined;
        });
    },

    // should a new client be created to receive part gets? or is it ok to
    // recursively call get inside itself?
    get: function getK(key, range, reqUids, callback) {
        console.log("range in backend", range);
        const testKinetic = config.kinetic.instance;
        const maxSize = 1048576;

        // check that part is isMaster
        let isMaster;

        if (range) {
            console.log("found range", range);
            const size = range[1] - range[0];
            isMaster = (size > maxSize) ? true : false;
        }
        else {
            console.log("range is not true:", range);
            isMaster = true; // change this later
            console.log("See if req. repeats");
        }

        if (isMaster) {
            let partSum;
            let masterValue = Buffer.allocUnsafe(0);
            // get the response from get request as stream; do to stream:
            return testKinetic.get(Buffer.from(key), range, (err, stream) => {
                stream.on("data", (chunk) => {
                    masterValue = Buffer.concat([masterValue, chunk]);
                }).on('end', err => {
                    // get keys for each part
                    const keysArray = _parseChunk(masterValue);
                    // get value for each part and aggregate
                    var valueTab = [];
                    async.eachSeries(keysArray, (key, cb) => {
                        let partValue = Buffer.allocUnsafe(0);
                        testKinetic.get(Buffer.from(key), range, (err, stream) => {
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
                        console.log(partSum);
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
            return testKinetic.get(Buffer.from(key), range, callback);
        }
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        return testKinetic.delete(key, callback);
    },
};

export default backend;
