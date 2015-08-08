var validator = require('is-my-json-valid')
var format = require('json-format-stream')
var filterObj = require('filter-object')
var dataSchema = require('data-schema')
var sublevel = require('subleveldown')
var through = require('through2')
var extend = require('extend')
var each = require('each-async')
var dat = require('dat-core')
var clone = require('clone')
var cuid = require('cuid')

var debug = require('debug')('datwrapper-model')

module.exports = Dats

function Dats (db, options) {
  if (!(this instanceof Dats)) return new Dats(db, options)
  options = options || {}
  this._db = db
  this.metadata = sublevel(db, 'dats-metadata', { valueEncoding: 'json' })
  this.authorize = options.authorize || function (permission, done) { done(null, true) }
  this.schema = clone(require('./schema'))
}

Dats.prototype.open = function (key, options, callback) {
  if (typeof key === 'object') {
    callback = options
    options = key
    key = options.key
  }

  var self = this
  options.key = key
  this.authorize(auth('read', key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    DatWrapper(self, options, function (err, datwrapper, metadata) {
      if (err) return callback(err)
      callback(null, datwrapper)
    })
  })
}

Dats.prototype.get = function (key, callback) {
  var self = this
  this.authorize(auth('read', key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self.metadata.get(key, callback)
  })
}

Dats.prototype.list =
Dats.prototype.createReadStream = function (options) {
  var self = this
  options = extend({ keys: true, values: false }, options)

  function map (key, enc, next) {
    var stream = this
    self.get(key, function (err, data) {
      if (err) return next(err)
      stream.push(data)
      next()
    })
  }

  return self.metadata.createReadStream(options).pipe(through.obj(map))
}

function DatWrapper (Dats, options, callback) {
  if (!(this instanceof DatWrapper)) return new DatWrapper(Dats, options, callback)

  if (typeof options === 'function') {
    callback = options
    options = {}
  }

  debug('DatWrapper constructor', options)

  var self = this
  this.key = options.key
  this.db = sublevel(Dats._db, 'Dats')
  this.Dats = Dats
  this.metadata = Dats.metadata
  this._metadata = {}
  this.schema = clone(require('./schema'))
  this.validate = validator(this.schema.metadata)
  this.authorize = options.authorize || Dats.authorize

  if (options.createIfMissing === undefined) options.createIfMissing = true
  if (options.createIfMissing) this.key = cuid()

  this.metadata.get(this.key, function (err, metadata) {
    if (err && !options.createIfMissing) return callback(new Error('Not Found'))
    metadata = metadata || {}

    self.rowSchema = dataSchema({ schema: self.schema.rows, properties: metadata.rowSchema || {} })
    self.dat = dat(sublevel(self.db, self.key), { valueEncoding: 'json' })
    self.rows = Rows(self)

    self.dat.on('error', function (err) {
      debug('dat init error', err)
      callback(err)
    })

    self.dat.on('ready', function () {
      self.authorize(auth('read', self.key), function (authErr, ok) {
        if (authErr || !ok) return callback(new Error('Not Authorized'))
        debug('dat ready', options)
        self.put(options, function (err, metadata) {
          if (err) return callback(err)
          self.rowSchema.addProperties(self.rowSchema.all())
          return callback(null, self)
        })
      })
    })
  })
}

DatWrapper.prototype.put =
DatWrapper.prototype.save = function (data, callback) {
  var key = data.key = data.key || this.key

  debug('DatWrapper.put key', key)
  debug('DatWrapper.put data', data)

  if (this.validate(data)) {
    if (!key) return this.create(data, callback)
    return this.update(key, data, callback)
  }

  callback(new Error('invalid data format'))
}

DatWrapper.prototype._put = function (key, data, callback) {
  var self = this
  this.key = key

  debug('DatWrapper._put', key, data)

  this._putMetadata(key, data, function () {
    self._putDat(key, data, function (err) {
      if (err) return callback(err)
      self.key = key
      callback(err, data)
    })
  })
}

DatWrapper.prototype._putMetadata = function (key, data, callback) {
  var self = this
  this.metadata.get(key, function (err, existing) {
    if (err) return callback(err)
    if (existing) data = extend(existing, data)
    data = filterObj(data, ['*', '!rows', '!createIfMissing'])
    self.metadata.put(key, data, function (err) {
      if (err) return callback(err)
      self._metadata = data
      return callback(null, data)
    })
  })
}

DatWrapper.prototype._putDat = function (key, data, callback) {
  if (!data.rows) return callback()
  var self = this

  each(data.rows, function (row, i, next) {
    self.rows.put(row, next)
  }, callback)
}

DatWrapper.prototype.create = function (data, callback) {
  var self = this
  this.authorize(auth('create', this.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    var key = data.key = self.key = cuid()
    self._put(key, data, callback)
  })
}

DatWrapper.prototype.update = function (key, data, callback) {
  var self = this
  this.authorize(auth('update', key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    var key = data.key = self.key
    debug('DatWrapper.update', key, data)
    self._put(key, data, callback)
  })
}

DatWrapper.prototype.get = function (callback) {
  var self = this
  var key = this.key
  this.authorize(auth('read', key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    debug('DatWrapper.get key', key)
    self.metadata.get(key, function (err, data) {
      callback(err, data)
    })
  })
}

DatWrapper.prototype.delete = function (callback) {
  var self = this
  this.authorize(auth('delete', this.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self.metadata.del(self.key, callback)

    /*
    // deleting all the rows takes forever:

    this.rows.createReadStream()
      .pipe(through.obj(function (chunk, enc, next) {
        self.delete(chunk, function () {
          next()
        })
      }, function end () {
        self.metadata.del(self.key, callback)
      }))
    */
  })
}

DatWrapper.prototype.createReadStream = function (options) {
  options = extend({ keys: true, values: true }, options)
  var stream = format(this._metadata, { outputKey: 'rows' })
  return this.dat.createReadStream(options).pipe(stream)
}

DatWrapper.prototype.updateSchema = function (callback) {
  var self = this
  this.authorize(auth('update', this.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self._putMetadata(self.key, { rowSchema: self.rowSchema.all() }, callback)
  })
}

DatWrapper.prototype.fullSchema = function () {
  var schema = this.schema.metadata
  schema.properties.rows = this.rowSchema.schema
  return schema
}

function Rows (datwrapper) {
  if (!(this instanceof Rows)) return new Rows(datwrapper)
  this.datwrapper = datwrapper
  this.dat = datwrapper.dat
  this.schema = datwrapper.rowSchema
  this.authorize = datwrapper.authorize
}

Rows.prototype.put = function (key, data, options, callback) {
  if (typeof key === 'object') return this.put(data.key, key, data, options)

  if (typeof options === 'function') {
    callback = options
    options = {}
  }

  if (key) return this.update(key, data, options, callback)
  return this.create(data, options, callback)
}

Rows.prototype._put = function (key, data, options, callback) {
  var self = this
  data = self.schema.format(data)

  this.datwrapper.updateSchema(function () {
    if (self.schema.validate(data)) {
      self.dat.put(key, data, function (err) {
        if (err) return callback(err)
        self.dat.get(key, function (err, row) {
          if (err) return callback(err)
          row.datKey = self.datwrapper.key
          row.schema = self.schema.all()
          if (options.names) row.value = self.schema.map('name', row.value)
          callback(null, row)
        })
      })
    }

    else callback(new Error('invalid row schema'))
  })
}

Rows.prototype.create = function (data, options, callback) {
  var self = this

  if (typeof options === 'function') {
    callback = options
    options = {}
  }

  this.authorize(auth('create', this.datwrapper.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    var key = cuid()
    self._put(key, data, options, callback)
  })
}

Rows.prototype.update = function (key, data, options, callback) {
  if (typeof key === 'object') return this.update(data.key, data, options, callback)
  if (typeof options === 'function') {
    callback = options
    options = {}
  }
  var self = this
  this.authorize(auth('update', this.datwrapper.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self._put(key, data, options, callback)
  })
}

Rows.prototype.get = function (key, options, callback) {
  var self = this

  if (typeof options === 'function') {
    callback = options
    options = {}
  }

  this.authorize(auth('read', this.datwrapper.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self.dat.get(key, options, function (err, row) {
      if (err) return callback(err)
      row.datKey = self.datwrapper.key
      row.schema = self.schema.all()
      if (options.names) row.value = self.schema.map('name', row.value)
      callback(null, row)
    })
  })
}

Rows.prototype.del =
Rows.prototype.delete = function (key, data, callback) {
  var self = this
  this.authorize(auth('delete', this.datwrapper.key), function (authErr, ok) {
    if (authErr || !ok) return callback(new Error('Not Authorized'))
    self.dat.del(key, callback)
  })
}

Rows.prototype.list =
Rows.prototype.createReadStream = function (options) {
  options = options || {}
  var self = this
  var stream = this.dat.createReadStream(options)
  if (options.names) {
    return stream.pipe(through.obj(function (chunk, enc, next) {
      chunk.value = self.schema.map('name', chunk.value)
      this.push(chunk)
      next()
    }))
  }
  return stream
}

function auth (action, key) {
  return { dats: { action: action, key: key } }
}
