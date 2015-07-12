var http = require('http')
var memdb = require('memdb')
var datWrapper = require('../index')()
var data = datWrapper(memdb())

var server = http.createServer(function (req, res) {
  if (data.serve(req, res)) return
  else res.end(JSON.stringify({ error: 'not found' }))
})

server.listen(4243)