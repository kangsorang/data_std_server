const express = require('express');
const app = express();
var generateConnectionTimelineData = require('./dataUtil').generateConnectionTimelineData;
const cors = require('cors');
let cachedData = new Map();
app.use(cors());

app.get('/', (req, res) => {
  res.send('Hello World!\n');
});

app.listen(2000, () => {
  console.log('Example app listening on port 2000!');
});

app.get("/getData", async (req, res, next) => {
  let shop = req.param("shop");
  console.log("GET request /getData shop : " + shop)
  if (shop == undefined) {
    //default to TOAL data
    shop = "TOTAL"
  }
  var sendData;
  if (cachedData.has(shop)) {
    //console.log("Use cached data")
    sendData = cachedData.get(shop)
  }
  else {
    console.log("Generate data")
    sendData = await generateConnectionTimelineData(shop)
  }
  cachedData.set(shop, sendData)
  let resData = JSON.stringify([...sendData])
  let parsedData = JSON.parse(resData);
  res.json(parsedData);
});