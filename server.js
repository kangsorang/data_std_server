const express = require('express');
const app = express();
var { generateConnectionTimelineByReportData, generateConnectionTimelineByIHData} = require('./dataUtil')
const cors = require('cors');
let cachedReportData = new Map();
let cachedIHData = new Map();
app.use(cors());

app.get('/', (req, res) => {
  res.send('Hello World!\n');
});

app.listen(2000, () => {
  console.log('Example app listening on port 2000!');
});

app.get("/getReportData", async (req, res, next) => {
  let shop = req.param("shop");
  console.log("GET request /getReportData shop : " + shop)
  if (shop == undefined) {
    //default to TOAL data
    shop = "TOTAL"
  }
  var sendData;
  if (cachedReportData.has(shop)) {
    //console.log("Use cached data")
    sendData = cachedReportData.get(shop)
  }
  else {
    console.log("Generate data")
    sendData = await generateConnectionTimelineByReportData(shop)
  }
  cachedReportData.set(shop, sendData)
  let resData = JSON.stringify([...sendData])
  let parsedData = JSON.parse(resData);
  res.json(parsedData);
});

app.get("/getIHData", async (req, res, next) => {
  let shop = req.param("shop");
  console.log("GET request /getIHData shop : " + shop)
  if (shop == undefined) {
    //default to TOAL data
    shop = "TOTAL"
  }
  var sendData;
  if (cachedIHData.has(shop)) {
    //console.log("Use cached data")
    sendData = cachedIHData.get(shop)
  }
  else {
    console.log("Generate data")
    sendData = await generateConnectionTimelineByIHData(shop)
  }
  cachedIHData.set(shop, sendData)
  let resData = JSON.stringify([...sendData])
  let parsedData = JSON.parse(resData);
  res.json(parsedData);
});