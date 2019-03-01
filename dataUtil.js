const fs = require('fs');
const es = require('event-stream');
const {TOTAL} = require('./shopData')
const JSON_EXTENSION = 'json'
var lineNr = 0;
var dataErrorCnt = 0;
const CONNECTION_TIMEOUT = 180//sec

/*
    Key : mac_sn
    Value : [[startTime, endTime]...]
*/
var connectionTimeLineData = new Map();

var dataPath = './raw_data/data_qv25'
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

function doReadFile(file, shop) {
    console.log("doReadFile Start : " + file + " shop : " + shop)
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
                        let event_time = parsedLineData.event_time// + 9 * 60 * 60; //adjust UTC
                        let mac_sn = parsedLineData.mac_sn;
                        
                        if (eval(shop).includes(parseInt(mac_sn))) {
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
                        console.log("Maybe too long data..... : " + exception)
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

function generateConnectionTimelineData(shop) {
    //before generate data, clear old data
    connectionTimeLineData.clear();
    return new Promise((resolve, reject) => {
        console.time("generateConnectionTimelineData")
        //step 1. search dir and marking target files
        searchDir(dataPath)
        console.table(targetFileList)
        
        //step 2. read files and set data
        targetFileList.reduce( async (previousPromise, file) => {
            await previousPromise;
            return doReadFile(file, shop);
        }, Promise.resolve())
        .then(() => {
            console.table(connectionTimeLineData)
            console.log("Error data count : " + dataErrorCnt)
            console.timeEnd("generateConnectionTimelineData")
            /*
            eval(shop).forEach(macsn => {
                if (!connectionTimeLineData.has('' + macsn)) {
                    connectionTimeLineData.set('' + macsn, [[]])
                }
            })
            */
            resolve(connectionTimeLineData)
        })
        .catch(() => {
            reject();
        })
    })
}

module.exports = {
    generateConnectionTimelineData
}