const express = require('express');
const app = express();
var data = require('./index.js').result;
const cors = require('cors');

app.use(cors());

app.get('/', (req, res) => {
  res.send('Hello World!\n');
 });

app.listen(2000, () => {
  console.log('Example app listening on port 2000!');
});

app.get("/test", (req, res, next) => {
    let resData = JSON.stringify([...data])
    let parsedData = JSON.parse(resData);
    res.json(parsedData);
});