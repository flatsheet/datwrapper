var qs = require('querystring')
var request = require('request')

module.exports = DatWrapperClient

function DatWrapperClient (options) {
  if (!(this instanceof DatWrapperClient)) return new DatWrapperClient(options)
  options = options || {}

  this.account = {
    username: options.username,
    password: options.password
  }

  this.host = options.host || 'http://127.0.0.1:4243'
  this.prefix = options.prefix || '/api/v1/data'
  this.rows = Rows(this)
  this.schema = Schema(this)
}

DatWrapperClient.prototype.request = function (method, path, params, cb) {
  if (typeof params === 'function') {
    cb = params
    params = {}
  }

  var options = {}

  if (method === 'get') {
    params = qs.stringify(params)
    options.uri = this.fullUrl(path, params)
  } else {
    options.uri = this.fullUrl(path)
    options.body = params
  }

  options.json = true
  options.method = method

  if (this.account.username && this.account.password) {
    options.headers = {
      'Authorization': this.account.username + ':' + this.account.password
    }
  }

  if (typeof cb === 'undefined') return request(options)
  else request(options, getResponse)

  function getResponse (error, response, body) {
    if (cb) {
      if (error) return cb(error)
      if (response.statusCode >= 400) return cb({ error: { status: response.statusCode } })
      return cb(null, body)
    }
  }
}

DatWrapperClient.prototype.get = function (key, options, cb) {
  this.request('get', '/' + key, options, cb)
}

DatWrapperClient.prototype.list = function (options, cb) {
  return this.request('get', '', options, cb)
}

DatWrapperClient.prototype.create = function (options, cb) {
  return this.request('post', '', options, cb)
}

DatWrapperClient.prototype.update = function (key, options, cb) {
  if (typeof key === 'object') {
    cb = options
    options = key
    key = options.key
  }
  return this.request('put', '/' + key, options, cb)
}

DatWrapperClient.prototype.delete = function (key, cb) {
  return this.request('delete', '/' + key, {}, cb)
}

DatWrapperClient.prototype.fullUrl = function fullUrl (path, params) {
  var url = this.host + this.prefix + path
  if (params) url += '?' + params
  return url
}

function Rows (client) {
  if (!(this instanceof Rows)) return new Rows(client)
  this.client = client
}

Rows.prototype.get = function (key, rowkey, cb) {
  return this.client.request('get', '/' + key + '/rows/' + rowkey, cb)
}

Rows.prototype.create = function (key, options, cb) {
  return this.client.request('post', '/' + key + '/rows', options, cb)
}

Rows.prototype.update = function (key, data, cb) {
  return this.client.request('put', '/' + key + '/rows/' + data.key, data.value, cb)
}

Rows.prototype.delete = function (key, rowkey, cb) {
  return this.client.request('delete', '/' + key + '/rows/' + rowkey, cb)
}

function Schema (client) {
  if (!(this instanceof Schema)) return new Schema(client)
  this.client = client
}

Schema.prototype.all = function (key, options, cb) {
  return this.client.request('get', '/' + key + '/schema', options, cb)
}

Schema.prototype.get = function (key, schemakey, cb) {
  return this.client.request('get', '/' + key + '/schema/' + schemakey, cb)
}

Schema.prototype.create = function (key, data, cb) {
  return this.client.request('post', '/' + key + '/schema', data, cb)
}

Schema.prototype.update = function (key, data, cb) {
  return this.client.request('put', '/' + key + '/schema/' + data.key, data.value, cb)
}

Schema.prototype.delete = function (key, schemakey, cb) {
  return this.client.request('delete', '/' + key + '/schema/' + schemakey, {}, cb)
}
