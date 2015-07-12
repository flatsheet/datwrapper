module.exports = function (handler, options) {
  var router = require('match-routes')()
  var prefix = options.prefix || '/api/v1/data'
  router.on(prefix + '/', handler.datwrapperList.bind(handler))
  router.on(prefix + '/:key', handler.datwrapper.bind(handler))
  router.on(prefix + '/:key/dat', handler.dat.bind(handler))
  router.on(prefix + '/:key/rows', handler.rows.bind(handler))
  router.on(prefix + '/:key/rows/:rowkey', handler.rowsKey.bind(handler))
  router.on(prefix + '/:key/schema', handler.schema.bind(handler))
  router.on(prefix + '/:key/schema/:schemakey', handler.schemaKey.bind(handler))
  return router
}
