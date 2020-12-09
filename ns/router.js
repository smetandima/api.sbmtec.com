var express = require('express');
var router = express.Router();

const bodyParser = require('body-parser')
const jsonParser = bodyParser.json()

const { getCustomerInfo } = require('./model')

router.get('/', function(req, res) {
  res.send('Root')
});

router.get('/api', function(req, res) {
  res.send('API')
});

router.post('/get_customer_info', jsonParser, async function(req, res){
  await getCustomerInfo(
    req.body.offset,
    req.body.limit,
    req.body.period,
    req.body.shop,
    req.body.search_key
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});

// export your router
module.exports = router;
