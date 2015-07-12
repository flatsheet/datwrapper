var each = require('each-async')
var test = require('tape')
var client = require('../api-client')()
var data = require('./fixtures/metadata')
var server = require('./helpers/server')

server.listen(4243, function () {
  test('create, get, and update a dataset', function (t) {
    client.create(data[0], getDataset)

    function getDataset (err, res) {
      t.notOk(err)
      t.ok(res)
      client.get(res.key, updateDataset)
    }

    function updateDataset (err, res) {
      t.notOk(err)
      t.ok(res)
      res.title = 'awesome'
      client.update(res.key, res, verifyDataset)
    }

    function verifyDataset (err, res) {
      t.notOk(err)
      t.ok(res)
      t.equal(res.title, 'awesome')
      t.end()
    }
  })

  test('list datasets', function (t) {
    client.list(function (err, res) {
      t.end()
    })
  })

  test('create, get, and update a row', function (t) {
    client.create(data[0], getDataset)

    function getDataset (err, res) {
      t.notOk(err)
      t.ok(res)
      client.rows.create(res.key, { agree: 'yes' }, getRow)
    }

    function getRow (err, res) {
      client.rows.get(res.datKey, res.key, updateRow)
    }

    function updateRow (err, res) {
      res.value.agree = 'no'
      client.rows.update(res.datKey, res, verifyRow)
    }

    function verifyRow (err, res) {
      t.notOk(err)
      t.ok(res)
      Object.keys(res.schema).forEach(function (key) {
        if (res.schema[key].name === 'agree') {
          t.equal(res.value[res.schema[key].key], 'no')
        }
      })
      t.end()
    }
  })

  test('create, get, update, and delete a schema', function (t) {
    client.create(data[3], getDataset)

    function getDataset (err, res) {
      t.notOk(err)
      t.ok(res)
      client.rows.create(res.key, { pizza: 'yes' }, getAllSchema)
    }

    function getAllSchema (err, res) {
      client.schema.all(res.datKey, updateSchema)
    }

    function updateSchema (err, res) {
      client.schema.create(res.datKey, { type: 'string', name: 'wow' }, getProperty)
    }

    function getProperty (err, res) {
      client.schema.get(res.datKey, res.property.key, deleteProperty)
    }

    function deleteProperty (err, res) {
      t.end()
    }

    function verifySchema (err, res) {
      
    }
  })

  test('delete datasets', function (t) {
    client.list(function (err, res) {
      each(res, function (dataset, i, next) {
        client.delete(dataset.key, function (err) {
          t.notOk(err)
          client.get(dataset.key, function (err, ds) {
            t.ok(err)
            t.notOk(ds)
            next()
          })
        })
      }, function () {
        server.close()
        t.end()
      })
    })
  })
})
