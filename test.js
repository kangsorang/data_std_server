const {CAROUSEL1, CAROUSEL2, qv25_d1} = require('./shopData')

var sum70 = CAROUSEL1.concat(CAROUSEL2)

sum70.forEach(item => {
    if (!qv25_d1.includes(item)) {
        console.log("Find!! : " + item)
    }
})