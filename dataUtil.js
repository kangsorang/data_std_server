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
//var reportDataPath = './data_std_server/raw_data/data_qv25/report'
//var ihDataPath = './data_std_server/raw_data/data_qv25/ih'
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

function doReadFile(file, shop, result, processFunc) {
    console.log("doReadFile Start : " + file + " shop : " + shop)
    return new Promise((resolve, reject) => {//TODO : reject case handling
        var s = fs.createReadStream(file)
            .pipe(es.split())
            .pipe(es.through(function write(data) {
                    s.pause();
                    processFunc(data, shop, result)
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

function processIhData(data, shop, resultArr) {
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
    /*
    { 
        "time": "2019-03-07T19:01:46Z", 
        "resourceId": "/SUBSCRIPTIONS/5E2610CE-41BF-42FC-9386-F22FD1C48177/RESOURCEGROUPS/QV25-RG-DEV/PROVIDERS/MICROSOFT.DEVICES/IOTHUBS/QV25-IH-DEV", 
        "operationName": "deviceDisconnect", 
        "category": "Connections", 
        "level": "Error", 
        "resultType": "404104", 
        "resultDescription": "DeviceConnectionClosedRemotely", 
        "properties": "{\"deviceId\":\"62175304442320\",\"protocol\":\"Mqtt\",\"authType\":null,\"maskedIpAddress\":\"174.203.3.XXX\",\"statusCode\":\"404\"}", 
        "location": "koreacentral"
    }
    */
    try {
        if (data.trim() == '') return;
        let parsedLineData = JSON.parse(data);
        
        let event_time = Math.floor(Date.parse(parsedLineData.time) / 1000) //1551957406031
        let operationName = parsedLineData.operationName
        let category = parsedLineData.category;
        let level = parsedLineData.level;
        let resultType = parsedLineData.resultType;
        let resultDescription = parsedLineData.resultDescription;
        let properties = JSON.parse(parsedLineData.properties)
        let mac_sn = properties.deviceId + IOTHUB_POSTFIX;
        let operationConnect = (parsedLineData.operationName == 'deviceConnect');
        
        if (!checkShopIncludes(shop, mac_sn)) {
            return;
        }

        resultArr.push({
            deviceId : mac_sn,
            event_time_vis : parsedLineData.time,
            event_time,
            operationName,
            category,
            level,
            resultType,
            resultDescription,
            properties
        })
    }
    catch(exception) {
        console.log("Maybe too long data..... : " + exception)
    }
}

function postProcessIhData(dataArr) {
    /*
    {
        deviceId: "132344196747055-ih",
        event_time_vis: "2019-03-07T18:16:15Z",
        event_time: 1551982575,
        operationName: "deviceDisconnect",
        category: "Connections",
        level: "Information",
        properties: {
            deviceId: "132344196747055",
            protocol: "Mqtt",
            authType: null,
            maskedIpAddress: "174.203.3.XXX",
            statusCode: null
        }
    },
    {
        deviceId: "132344196747055-ih",
        event_time_vis: "2019-03-07T19:02:52Z",
        event_time: 1551985372,
        operationName: "deviceDisconnect",
        category: "Connections",
        level: "Error",
        resultType: "404104",
        resultDescription: "DeviceConnectionClosedRemotely",
        properties: {
            deviceId: "132344196747055",
            protocol: "Mqtt",
            authType: null,
            maskedIpAddress: "174.203.3.XXX",
            statusCode: "404"
        }
    }...
    */
    let resultMap = new Map();
    let connFlag = false;
    for (var i = 0; i < dataArr.length; i++) {
        let item = dataArr[i];
        let mac_sn = item.deviceId;
        //console.log(">>" + mac_sn)
        let event_time = item.event_time;
        if (resultMap.has(mac_sn)) {
            let timeDataArr = resultMap.get(mac_sn);
            if (item.operationName == 'deviceConnect' && item.level !== "Error") {
                if (connFlag) {
                    
                }
                else {
                    //resultMap.get(mac_sn).push([event_time, event_time])
                }
                connFlag = true;
            }
            else {
                if (connFlag) {
                    let cursor = timeDataArr[timeDataArr.length - 1]
                    cursor[1] = event_time
                }
                else {
                    //
                }
                connFlag = false;
            }
        }
        else {
            if (item.operationName == 'deviceConnect' && item.level !== "Error") {
                resultMap.set(mac_sn, [[event_time, event_time]]);
                connFlag = true;
            }
            else {
                connFlag = false;
            }
        }
    }
    return resultMap;
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
        //let reportData = new Map();
        let ihDataArr = []
        ihFileList.reduce( async (previousPromise, file) => {
            await previousPromise;
            return doReadFile(file, shop, ihDataArr, processIhData);
        }, Promise.resolve())
        .then(() => {
            let resultMap = postProcessIhData(ihDataArr)
            console.table(resultMap)
            console.timeEnd("generateConnectionTimelineByIHData")
            resolve(resultMap)
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