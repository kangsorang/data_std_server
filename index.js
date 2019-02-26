const fs = require('fs');
const es = require('event-stream');

const JSON_EXTENSION = 'json'
var lineNr = 0;
var dataErrorCnt = 0;
const CONNECTION_TIMEOUT = 180//sec

/*
    Key : mac_sn
    Value : [[startTime, endTime]...]
*/
var connectionTimeLineData = new Map();

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

var dataPath = './data'
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

function doReadFile(file) {
    console.log("doReadFile Start : " + file)
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
                    try {
                        let parsedLineData = JSON.parse(data);
                        let event_time = parsedLineData.event_time + 9 * 60 * 60; //adjust UTC
                        let mac_sn = parsedLineData.mac_sn;
    
                        if (filterItem.includes(parseInt(mac_sn))) {
                            if (connectionTimeLineData.has(mac_sn)) {
                                let timeDataArr = connectionTimeLineData.get(mac_sn);
                                let cursor = timeDataArr[timeDataArr.length - 1];
                                let startTime = cursor[0];
                                let endTime = cursor[1];
                                if (endTime < event_time) {
                                    connectionTimeLineData.get(mac_sn).push([event_time, event_time + CONNECTION_TIMEOUT])
                                }
                                else if(startTime < event_time && endTime > event_time) {
                                    cursor[1] = event_time + CONNECTION_TIMEOUT
                                }
                            }
                            else {
                                connectionTimeLineData.set(mac_sn, [[event_time, event_time + CONNECTION_TIMEOUT]]);
                            }
                        }
                    }
                    catch(exception) {
                        console.log("Maybe too long data.....")
                        dataErrorCnt++
                    }
                    s.resume(); 
                },
                function end () {
                    //this.emit('end')
                    s.destroy();
                    console.log("doReadFile End : " + file)
                    resolve();
                })
            )
    })
}

searchDir(dataPath)
console.table(targetFileList)

targetFileList.reduce( async (previousPromise, file) => {
    await previousPromise;
    return doReadFile(file);
}, Promise.resolve())
.then(() => {
    console.table(connectionTimeLineData)
    console.log("Error data count : " + dataErrorCnt)
    require('./server.js')
})

/*
targetFileList.reduce((previousPromise, file) => {
    return previousPromise.then(() => {
        return new doReadFile(file);
    });
}, Promise.resolve())
.then(() => {
    console.table(connectionTimeLineData)
    require('./server.js')
})
*/
module.exports = {
    connectionTimeLineData
}