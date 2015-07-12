# datwrapper

Manage a bunch of datasets via JS or API with versioning, dynamic schemas, and that are compatible & replicable with [Dat](http://dat-data.com) instances. Built for use in [flatsheet](http://github.com/flatsheet/flatsheet).

[![Build Status](https://travis-ci.org/flatsheet/datwrapper.svg)](https://travis-ci.org/flatsheet/datwrapper)

## Install

```
npm install --save datwrapper
```

## Overview

The datwrapper router provides these routes:

```
GET /data
POST /data
GET /data/:key
PUT /data/:key
DELETE /data/:key

POST /data/:key/dat

GET /data/:key/rows
POST /data/:key/rows
GET /data/:key/rows/:rowkey
PUT /data/:key/rows/:rowkey
DELETE /data/:key/rows/:rowkey

GET /data/:key/schema
POST /data/:key/schema
GET /data/:key/schema/:schemakey
POST /data/:key/schema/:schemakey
DELETE /data/:key/schema/:schemakey
```

## Example usage

### Create a server to expose the API endpoints:

```
var createDataServer = require('datwrapper')
var level = require('level')
var http = require('http')

var db = level('db')
var data = createDataServer()
var server = http.createServer(function (req, res) {
  if (data.serve(req, res)) return
  // else serve fallback/error route
})
```

### Require the model to use as a JS library:

```
var level = require('level')
var dataWrapper = require('datwrapper/model')
var db = level('db')
var data = dataWrapper(db)
```

## License

[MIT](LICENSE.md)
