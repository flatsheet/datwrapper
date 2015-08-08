module.exports = function (options) {
  options = options || {}

  return function (server) {
    var model = require('./model')(server.db, options)
    var handler = require('./handler')(model, options)
    var router = require('./router')(handler, options)

    model.name = 'datwrapper'
    model.handler = handler
    model.router = router
    model.schema = require('./schema/metadata')
    model.serve = function (req, res) {
      return router.match(req, res)
    }

    return model
  }
}
