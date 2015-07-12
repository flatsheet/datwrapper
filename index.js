module.exports = function (options) {
  options = options || {}

  return function (server) {
    var model = require('./model')(server.db, options)
    var handler = require('./handler')(model, options)
    var router = require('./router')(handler, options)

    return {
      name: 'data',
      model: model,
      schema: require('./schema/metadata'),
      handler: handler,
      router: router,
      serve: function (req, res) {
        return router.match(req, res)
      }
    }
  }
}
