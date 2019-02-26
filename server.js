const express = require('express');
const app = express();
var generateConnectionTimelineData = require('./dataUtil').generateConnectionTimelineData;
const cors = require('cors');
let cachedData = null;
app.use(cors());

app.get('/', (req, res) => {
  res.send('Hello World!\n');
});

app.listen(2000, () => {
  console.log('Example app listening on port 2000!');
});

app.get("/test", async (req, res, next) => {
  var sendData;
  if (cachedData) {
    console.log("Use cached data")
    sendData = cachedData
  }
  else {
    console.log("Generate data")
    sendData = await generateConnectionTimelineData()
  }
  cachedData = sendData
  let resData = JSON.stringify([...sendData])
  let parsedData = JSON.parse(resData);
  res.json(parsedData);
});