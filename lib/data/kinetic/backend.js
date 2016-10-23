import config from '../../Config';
import async from 'async';
// function x (err, key) {
//     console.log(err)
//     console.log()
// }

const maxSize = 1048576;

function splitData(totalValue, size) {
    const n_partitions = size / maxSize;
    const dataArray = [];

    for (let i = 0; i < n_partitions; i++) {
	dataArray.push(totalValue.slice(maxSize * i, maxSize * (i + 1)))
	console.log('SIZE')
	console.log(totalValue.slice(maxSize * i, maxSize * (i + 1)).length)
    }
    console.log(dataArray);
    console.log("##################################################");
    console.log("##################################################");
    console.log("##################################################");
    console.log("##################################################");
    console.log("##################################################");

    return dataArray;
}

function joinBuffers(buffers, delimiter = ' ') {
  let d = Buffer.from(delimiter);

  return buffers.reduce((prev, b) => Buffer.concat([prev, d, b]));
}

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        const value = [];
        const testKinetic = config.kinetic.instance; // spin up a Kinetic class object
        request.on('data', data => {		     // get data from request
            value.push(data);
        }).on('end', err => {
	    if (size > maxSize) {
		if (err) {
		    return callback (err);
		}
		const totalValue = Buffer.concat(value);
		const keyTab = [];
		const dataArray = splitData(totalValue, size);
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
		}, (err) => {
		    const masterKey = joinBuffers(keyTab, '|');
		    console.log(masterKey.toString("hex"))
		    testKinetic.put(masterKey, {}, callback)
		});
		// const masterKey = Buffer.concat(keyTab);
		// testKinetic.put(masterKey, {}, callback)
	    } else {
		if (err) {
                    return callback(err);
		}
		testKinetic.put(value, {}, callback);
	    }
            return undefined;
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        return testKinetic.get(Buffer.from(key), range, callback);
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        return testKinetic.delete(key, callback);
    },
};

export default backend;
