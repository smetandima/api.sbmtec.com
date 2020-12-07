const express = require('express')
const path = require('path')
const app = express()
const cors = require('cors')

const router = require('./ns/router')

app.use(cors())
app.use('/', router);

// default Heroku PORT
app.listen(process.env.PORT || 3001, function(){
    console.log('App is running on http://localhost:3001')
});
