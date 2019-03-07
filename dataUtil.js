const fs = require('fs');
const es = require('event-stream');
const {TOTAL} = require('./shopData')
const JSON_EXTENSION = 'json'
const CONNECTION_TIMEOUT = 180//sec

const REPORT_POSTFIX = "-rp"
const IOTHUB_POSTFIX = "-ih"

/*
    Key : mac_sn
    Value : [[startTime, endTime]...]
*/
var reportDataPath = './raw_data/data_qv25/report'
var ihDataPath = './raw_data/data_qv25/ih'

function searchDir(path, ext) {
    let items = fs.readdirSync(path)
    items.sort();
    items.forEach(item => {
        if (isDirectory(path + '/' + item)) {
            searchDir(path + '/' + item, ext)
        }
        else {
            if (getFileExtension(item) == ext) {
                fileList.push(path + '/' + item)
            }
        }
    })
}

var fileList = []
function makeFileList(path, ext) {
    fileList.length = 0; // clear array before work
    searchDir(path, ext);
    return [...fileList] //assign result array
}

function isDirectory(path) {
    return fs.statSync(path).isDirectory();
}

function getFileExtension(fileName) {
    return fileName.split('.').pop();
}

function doReadFile(file, shop, resultMap, processFunc) {
    console.log("doReadFile Start : " + file + " shop : " + shop)
    return new Promise((resolve, reject) => {//TODO : reject case handling
        var s = fs.createReadStream(file)
            .pipe(es.split())
            .pipe(es.through(function write(data) {
                    s.pause();
                    processFunc(data, shop, resultMap)
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

function processReportData(data, shop, resultMap) {
    /*
        event_time:1550879998
        mac_sn:"190090202749367"
    */
    try {
        if (data.trim() == '') return;
        let parsedLineData = JSON.parse(data);
        let event_time = parsedLineData.event_time// + 9 * 60 * 60; //adjust UTC
        let mac_sn = parsedLineData.mac_sn + REPORT_POSTFIX;
        
        if (!checkShopIncludes(shop, mac_sn)) {
            return;
        }

        if (resultMap.has(mac_sn)) {
            let timeDataArr = resultMap.get(mac_sn);
            let cursor = timeDataArr[timeDataArr.length - 1];
            let startTime = cursor[0];
            let endTime = cursor[1];
            if (endTime < event_time) {
                resultMap.get(mac_sn).push([event_time, event_time + CONNECTION_TIMEOUT])
            }
            else if(startTime < event_time && endTime > event_time) {
                cursor[1] = event_time + CONNECTION_TIMEOUT
            }
        }
        else {
            resultMap.set(mac_sn, [[event_time, event_time + CONNECTION_TIMEOUT]]);
        }
    }
    catch(exception) {
        console.log("Maybe too long data..... : " + exception)
    }
}

function processIhData(data, shop, resultMap) {
    /*
    {
        "time": "2019-03-07T00:00:26Z",
        "resourceId": "/SUBSCRIPTIONS/5E2610CE-41BF-42FC-9386-F22FD1C48177/RESOURCEGROUPS/QV25-RG-DEV/PROVIDERS/MICROSOFT.DEVICES/IOTHUBS/QV25-IH-DEV",
        "operationName": "deviceConnect",
        "category": "Connections",
        "level": "Information",
        "properties": "{\"deviceId\":\"132344196746253\",\"protocol\":\"Mqtt\",\"authType\":null,\"maskedIpAddress\":\"174.203.17.XXX\",\"statusCode\":null}",
        "location": "koreacentral"
    }
    */
    try {
        if (data.trim() == '') return;
        let parsedLineData = JSON.parse(data);
        let event_time = Math.floor(Date.parse(parsedLineData.time) / 1000) //1551957406031
        let properties = JSON.parse(parsedLineData.properties)
        let mac_sn = properties.deviceId + IOTHUB_POSTFIX;
        let operationConnect = (parsedLineData.operationName == 'deviceConnect');
        
        if (!checkShopIncludes(shop, mac_sn)) {
            return;
        }

        if (resultMap.has(mac_sn)) {
            let timeDataArr = resultMap.get(mac_sn);
            if (operationConnect) {
                resultMap.get(mac_sn).push([event_time, event_time])
            }
            else {
                let cursor = timeDataArr[timeDataArr.length - 1]
                cursor[1] = event_time
            }
        }
        else {
            if (operationConnect) {
                resultMap.set(mac_sn, [[event_time, event_time]]);
            }
        }
    }
    catch(exception) {
        console.log("Maybe too long data..... : " + exception)
    }
}

function checkShopIncludes(shop, mac_sn) {
    return eval(shop).includes(parseInt(mac_sn))
}

/*
jsonReportFileList.reduce((previousPromise, file) => {
    return previousPromise.then(() => {
        return new doReadFile(file);
    });
}, Promise.resolve())
.then(() => {
    console.table(connectionTimeLineData)
    require('./server.js')
})
*/

function generateConnectionTimelineByReportData(shop) {
    return new Promise((resolve, reject) => {
        console.time("generateConnectionTimelineByReportData")
        //step 1. search dir and marking target files
        let reportFileList = makeFileList(reportDataPath, JSON_EXTENSION)
        
        console.table(reportFileList)
        
        //step 2. read files and set data
        let reportData = new Map();
        reportFileList.reduce( async (previousPromise, file) => {
            await previousPromise;
            return doReadFile(file, shop, reportData, processReportData);
        }, Promise.resolve())
        .then(() => {
            console.table(reportData)
            console.timeEnd("generateConnectionTimelineByReportData")
            /*
            eval(shop).forEach(macsn => {
                if (!connectionTimeLineData.has('' + macsn)) {
                    connectionTimeLineData.set('' + macsn, [[]])
                }
            })
            */
            resolve(reportData)
        })
        .catch(() => {
            reject();
        })
    })
}

function generateConnectionTimelineByIHData(shop) {
    return new Promise((resolve, reject) => {
        console.time("generateConnectionTimelineByIHData")
        //step 1. search dir and marking target files
        let ihFileList = makeFileList(ihDataPath, JSON_EXTENSION)

        console.table(ihFileList)
        
        //step 2. read files and set data
        let reportData = new Map();
        ihFileList.reduce( async (previousPromise, file) => {
            await previousPromise;
            return doReadFile(file, shop, reportData, processIhData);
        }, Promise.resolve())
        .then(() => {
            console.table(reportData)
            console.timeEnd("generateConnectionTimelineByIHData")
            /*
            eval(shop).forEach(macsn => {
                if (!connectionTimeLineData.has('' + macsn)) {
                    connectionTimeLineData.set('' + macsn, [[]])
                }
            })
            */
            resolve(reportData)
        })
        .catch(() => {
            reject();
        })
    })
}

module.exports = {
    generateConnectionTimelineByReportData,
    generateConnectionTimelineByIHData
}