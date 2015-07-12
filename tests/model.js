var test = require('tape')
var each = require('each-async')
var memdb = require('memdb')
var Dats = require('../model')

function create (index, cb) {
  if (typeof index === 'function') {
    cb = index
    index = 0
  }
  var data = require('./fixtures/metadata')[index]
  var datasets = Dats(memdb())
  datasets.open(data, cb)
  return datasets
}

test('create a dataset', function (t) {
  create(function (err, dataset) {
    t.notOk(err)
    t.ok(dataset)
    t.ok(dataset.key)
    dataset.get(function (err, metadata) {
      t.end()
    })
  })
})

test('update a dataset', function (t) {
  create(function (err, dataset) {
    dataset.get(function (err, metadata) {
      metadata.title = 'awesome dataset'
      dataset.put(metadata, function (err, updated) {
        t.equals(updated.title, 'awesome dataset')
        t.end()
      })
    })
  })
})

test('list datasets', function (t) {
  var count = 0
  var datasets = Dats(memdb())
  datasets.open({ title: '1', rows: [{ wat: 1 }] }, function () {
    datasets.open({ title: '2' }, function () {
      datasets.open({ title: '3' }, function () {
        datasets.createReadStream()
          .on('data', function (data) { count++ })
          .on('end', function () {
            t.equal(count, 3)
            t.end()
          })
      })
    })
  })
})

test('dataset has a schema for metadata', function (t) {
  create(function (err, dataset) {
    t.ok(dataset.schema.metadata)
    t.equals(dataset.schema.metadata.title, 'datwrapper')
    t.end()
  })
})

test('dataset has a schema for rows', function (t) {
  create(function (err, dataset) {
    t.ok(dataset.schema.rows)
    t.equals(dataset.schema.rows.title, 'Rows')
    t.end()
  })
})

test('update the row schema of a dataset', function (t) {
  create(function (err, dataset) {
    var props = [
      {
        name: '1',
        type: 'string'
      },
      {
        name: '2',
        type: 'number',
        default: 1010101
      }
    ]

    dataset.rowSchema.addProperties(props)

    dataset.updateSchema(function (err, metadata) {
      var count = 0
      for (key in metadata.rowSchema) {
        var prop = metadata.rowSchema[key]
        t.ok(prop.name)
        t.ok(prop.key)
        t.ok(prop.type)
        count++
      }
      t.equal(count, 2)
      t.end()
    })
  })
})

test('create a dataset with some rows', function (t) {
  var count = 0
  create(1, function (err, dataset) {
    dataset.rows.createReadStream()
      .on('data', function (data) { 
        count++
        t.ok(data.key)
        t.ok(data.version)
        t.ok(data.content)
        t.ok(data.value)
      })
      .on('end', function () {
        t.equal(count, 2)
        t.end()
      })
  })
})

test('update a row', function (t) {
  create(1, function (err, dataset) {
    var data = { pizza: 'fooood' }
    dataset.rows.put('1', data, function (err) {
      dataset.rows.get('1', { names: true }, function (err, row) {
        row.value.pizza = 'delicious'
        row.value.wat = 123.8
        dataset.rows.put('1', row.value, function (err) {
          dataset.rows.get('1', { names: true }, function (err, updated) {
            t.equal(updated.value.pizza, 'delicious')
            t.equal(updated.value.wat, 123.8)
            t.end()
          })
        })
      })
    })
  })
})

test('rows should all have same properties', function (t) {
  create(2, function (err, dataset) {
    dataset.rows.createReadStream({ names: true })
      .on('data', function (data) {
        for (key in data.value) {
          var value = data.value[key] === null || data.value[key] === 0
          t.ok(value)
        }
      })
      .on('end', t.end)
  })
})