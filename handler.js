var qs = require('querystring')
var response = require('response')
var JSONStream = require('JSONStream')
var parseBody = require('body/json')
var through = require('through2')
var filter = require('filter-object')
var extend = require('extend')
var pump = require('pump')

module.exports = function (datwrappers, params) {
  var handler = {}

  function openDatWrapper (options, callback) {
    options.createIfMissing = options.createIfMissing || false
    datwrappers.open(options, function (err, datwrapper) {
      if (err) return callback(err)
      datwrapper.get(function (err, metadata) {
        if (err) return callback(err)
        callback(null, datwrapper, metadata)
      })
    })
  }

  handler.datwrapperList = function (req, res, options) {
    if (req.method === 'GET') {
      datwrappers.list()
        .pipe(JSONStream.stringify())
        .pipe(res)
    }

    if (req.method === 'POST') {
      parseBody(req, res, function (err, body) {
        body.createIfMissing = true
        openDatWrapper(body, function (err, datwrapper, metadata) {
          if (err) response().json({ error: err }).status(400).pipe(res)
          response().json(metadata).pipe(res)
        })
      })
    }
  }

  handler.datwrapper = function (req, res, options) {
    if (req.method === 'GET') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        if (err) response().json({ error: 'Not Found' }).status(404).pipe(res)
        response().json(metadata).pipe(res)
      })
    }

    if (req.method === 'PUT') {
      parseBody(req, res, function (err, body) {
        openDatWrapper({ key: options.params.key }, function (err, datwrapper) {
          datwrapper.update(datwrapper.key, body, function (err, metadata) {
            if (err) response().json({ error: err }).status(400).pipe(res)
            response().json(metadata).pipe(res)
          })
        })
      })
    }

    if (req.method === 'DELETE') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        datwrapper.delete(function (err) {
          if (err) response().json({ error: err }).status(400).pipe(res)
          response().json({ message: 'deleted' }).status(200).pipe(res)
        })
      })
    }
  }

  handler.dat = function (req, res, options) {
    var readonly = options.query.readonly
    openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
      var dat = datwrapper.dat
      pump(req, readonly ? dat.push() : dat.replicate(), res)
    })
  }

  handler.rows = function (req, res, options) {
    if (req.method === 'GET') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        datwrapper.rows.createReadStream()
          .pipe(JSONStream.stringify())
          .pipe(res)
      })
    }

    if (req.method === 'POST') {
      parseBody(req, res, function (err, body) {
        openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
          datwrapper.rows.put(body, function (err, row) {
            if (err) response().json({ error: err }).status(400).pipe(res)
            response().json(row).pipe(res)
          })
        })
      })
    }
  }

  handler.rowsKey = function (req, res, options) {
    if (req.method === 'GET') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        if (err) return response().json({ error: err }).status(404).pipe(res)
        datwrapper.rows.get(options.params.rowkey, function (err, row) {
          if (err) response().json({ error: err }).status(400).pipe(res)
          response().json(row).pipe(res)
        })
      })
    }

    if (req.method === 'PUT') {
      parseBody(req, res, function (err, body) {
        openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
          datwrapper.rows.put(options.params.rowkey, body, function (err, row) {
            if (err) response().json({ error: err }).status(400).pipe(res)
            response().json(row).pipe(res)
          })
        })
      })
    }

    if (req.method === 'DELETE') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        datwrapper.rows.delete(options.params.rowkey, function (err, row) {
          if (err) response().json({ error: err }).status(400).pipe(res)
          response().json({ message: 'deleted' }).status(200).pipe(res)
        })
      })
    }
  }

  handler.schema = function (req, res, options) {
    if (req.method === 'GET') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        var obj = { datKey: options.params.key, schema: datwrapper.fullSchema() }
        response.json(obj).pipe(res)
      })
    }

    if (req.method === 'POST') {
      parseBody(req, res, function (err, body) {
        openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
          datwrapper.rowSchema.addProperty(body)
          datwrapper.updateSchema(function () {
            var obj = { datKey: datwrapper.key, property: datwrapper.rowSchema.find(body) }
            response.json(obj).pipe(res)
          })
        })
      })
    }
  }

  handler.schemaKey = function (req, res, options) {
    if (req.method === 'GET') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        var obj = { datKey: datwrapper.key, property: datwrapper.rowSchema.find(options.params.schemakey) }
        if (!obj.property) return response.json({ error: 'Not Found'}).status(404).pipe(res)
        response.json(obj).pipe(res)
      })
    }

    if (req.method === 'PUT') {
      parseBody(req, res, function (err, body) {
        openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
          datwrapper.updateSchema([body], function () {
            var obj = { datKey: datwrapper.key, property: datwrapper.rowSchema.find(body) }
            response.json(obj).pipe(res)
          })
        })
      })
    }

    if (req.method === 'DELETE') {
      openDatWrapper({ key: options.params.key }, function (err, datwrapper, metadata) {
        var prop = datwrapper.rowSchema.delete(options.params.schemakey)
        if (err) response().json({ error: err }).status(400).pipe(res)
        response().json({ message: 'deleted' }).status(200).pipe(res)
      })
    }
  }

  return handler
}