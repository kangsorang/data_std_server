const fs = require('fs');
const es = require('event-stream');
const util = require('util');
var async = require("async");

const JSON_EXTENSION = 'json'
var lineNr = 0;
var result = new Map();
/*
    Key : mac_sn
    Value : [time...]
*/

const filterItem = [
    132344196746872,
    132344196746405,
    132344196746470,
    132344196746725,
    132344196746211,
    132344196746841,
    132344196746568,
    132344196746784,
    132344196746231,
    132344196746448,
    132344196746453,
    132344196746532,
    132344196746341,
    132344196746165,
    132344196746743,
    132344196747004,
    132344196746250,
    132344196747055,
    132344196747006,
    132344196746429,
    132344196746486,
    132344196746878,
    132344196746630,
    132344196746629,
    132344196746652,
    132344196746315,
    132344196746932,
    132344196746935,
    132344196746426,
    132344196746975,
    132344196746508,
    132344196746764,
    62175304442320,
    132344196746952,
    132344196747020,
    132344196746917,
    132344196746857,
    132344196746951,
    132344196746297,
    132344196746845,
    132344196746687,
    132344196746760,
    132344196746590,
    132344196747016,
    132344196746383,
    132344196746556,
    132344196746668,
    132344196746796,
    132344196746988,
    132344196746215,
    132344196746168,
    132344196746190,
    132344196746253,
    132344196746514,
    132344196746973,
    132344196746318,
    132344196746554,
    132344196746912,
    132344196746588,
    132344196746135,
    132344196746232,
    132344196746530,
    132344196746894,
    132344196746469,
    184872542420654,
    132344196746805,
    184872542420641,
    132344196746707,
    132344196746985,
    132344196746489
]

var dataPath = './data/2019-02-23'
var stat;
var targetFileList = [];
function searchDir(path) {
    let items = fs.readdirSync(path)
    items.forEach(item => {
        if (isDirectory(path + '/' + item)) {
            searchDir(path + '/' + item)
        }
        else {
            if (getFileExtension(item) == JSON_EXTENSION) {
                targetFileList.push(path + '/' + item)
            }
        }
    })
}

function isDirectory(path) {
    return fs.statSync(path).isDirectory();
}

function getFileExtension(fileName) {
    return fileName.split('.').pop();
}

async function doReadFileSync(file) {
    await doReadFile(file);
}

function doReadFile(file) {
    console.log("doReadFile : " + file)
    return new Promise((resolve, reject) => {
        var s = fs.createReadStream(file)
            .pipe(es.split())
            .pipe(es.through(function write(data) {
                    s.pause();
                    /*
                        event_time:1550879998
                        mac_sn:"190090202749367"
                    */
                    lineNr++;
                    let parsedLineData = JSON.parse(data);
                    let event_time = parsedLineData.event_time;
                    let mac_sn = parsedLineData.mac_sn;
                    if (filterItem.includes(parseInt(mac_sn))) {
                        if (result.has(mac_sn)) {
                            let timeData = result.get(mac_sn)
                            if( timeData[timeData.length - 1] != event_time) {
                                timeData.push(event_time)
                                result.set(mac_sn, timeData);
                            }
                        }
                        else {
                            result.set(mac_sn, [event_time]);
                        }
                    }
                    s.resume(); 
                },
                function end () { //optional
                    //this.emit('end')
                    resolve();
                    s.destroy();
                })
            )
    })
}


console.log("START")
searchDir(dataPath)
console.log("==== targetFileList ====")
console.table(targetFileList)
var promiseList = [];
targetFileList.forEach(file => {
    promiseList.push(doReadFile(file))
})
Promise.all(promiseList)
.then(() => {
    console.table(result)
    require('./server.js')
})


module.exports = {
    result
}