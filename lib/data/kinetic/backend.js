import config from '../../Config';
import async from 'async';
// function x (err, key) {
//     console.log(err)
//     console.log()
// }
var bufferIndexOf = require('buffer-indexof');
const maxSize = 1048576;
const prependMaster = "part|";

// function splitBuffer(buf, delimiter) {
//     var arr = [], p = 0;

//     for (var i = 0, l = buf.length; i < l; i++) {
// 	if (buf[i] !== delimiter) continue;
// 	if (i === 0) {
// 	    p = 1;
// 	    continue; // skip if it's at the start of buffer
// 	}
// 	arr.push(buf.slice(p, i));
// 	p = i + 1;
//     }

//     // add final part
//     if (p < l) {
// 	arr.push(buf.slice(p, l));
//     }

//     return arr;
// }

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
	    console.log('==========================================================');
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
		    testKinetic.put(masterKey, {}, (err, key) => {
			const prepend = Buffer.from(prependMaster)
			const newKey = Buffer.concat([prepend, key]);
			console.log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			console.log(newKey.toString());
			console.log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			return callback(err, newKey);
		    })
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

    get: function getK(key, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        return testKinetic.get(Buffer.from(key), range, callback);
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
	const prepend = Buffer.from(prependMaster)

	console.log(prependMaster)
	
	if (key.slice(0, 5).toString() === prependMaster) {
	    // do code for deleting master

	    let masterKey = Buffer.allocUnsafe(0);
	    console.log('-----------------KEY----------------------------');
	    console.log(key.toString());
	    console.log('-----------------KEY----------------------------');
	    testKinetic.get(key.slice(5), undefined, (err, stream) => {
	    console.log("$$$$$ 1111345678")
		stream.on('data', chunk => {
		    console.log("$$$$$ 2222")
		    masterKey = Buffer.concat([masterKey, chunk])
		}).on('end', err => {
		    console.log("$$$$$ 3333")
		    if (err) {
			console.log("$$$$$ 4444")
			return callback(err);
		    }
		    
		    const keyTab = splitBuffer(masterKey, Buffer.from("|"));

		    console.log("$$$$$ 5555")

		    async.eachSeries(keyTab, (key, cb) => {
			console.log("$$$$$ 6666")
			console.log(keyTab);
			testKinetic.delete(key, err => {
			    console.log(err);
			    cb(err);
			});
		    }, (err) => {
			console.log("$$$$$ 7777")
			console.log(key.toString());
			testKinetic.delete(key.slice(5), err => {
			    console.log(err);
			    console.log("$$$$$ 8888")
			    return callback(err);			    
			});
		    });
		});
	    });
	    
	} else {
            return testKinetic.delete(key, callback);
	}

	return undefined;
    },
};

export default backend;
