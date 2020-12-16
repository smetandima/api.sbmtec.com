var express = require('express');
var router = express.Router();

const bodyParser = require('body-parser')
const jsonParser = bodyParser.json()

const {
  getCustomerVisitInfo,
  getCustomerVisitsCount,
  getCustomerVisitsCountCompare,
  getAllTimeVisitCount,
  getAllCustomersCount,
  getTopSaleItems,
  getCustomerVisitsCountByDate
} = require('./model')

router.get('/', function(req, res) {
  res.send('Root')
});

router.get('/api', function(req, res) {
  res.send('API')
});

router.post('/get_customer_visit_info', jsonParser, async function(req, res){
  await getCustomerVisitInfo(
    req.body.offset,
    req.body.limit,
    req.body.customer_type,
    req.body.shop,
    req.body.search_key
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});

router.post('/get_customer_visit_count', jsonParser, async function(req, res){
  await getCustomerVisitsCount(
    req.body.period,
    req.body.shop
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});

router.post('/get_customer_visit_count_by_date', jsonParser, async function(req, res){
  await getCustomerVisitsCountByDate(
    req.body.period,
    req.body.shop
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});

router.post('/get_customer_visit_count_compare', jsonParser, async function(req, res){
  await getCustomerVisitsCountCompare(
    req.body.period,
    req.body.shop
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});

router.post('/get_all_time_visit_count', jsonParser, async function(req, res){
  await getAllTimeVisitCount(
    req.body.shop
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});
router.post('/get_all_customer_count', jsonParser, async function(req, res){
  await getAllCustomersCount(
    req.body.shop
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});
router.post('/get_top_sale_items', jsonParser, async function(req, res){
  await getTopSaleItems(
    req.body.shop,
    req.body.period,
    req.body.offset,
    req.body.limit
  ).then(result => {
    res.status(200).send(result)
  }).catch(err => {
    res.status(500).send('Internal server error')
  })
});
// export your router
module.exports = router;
