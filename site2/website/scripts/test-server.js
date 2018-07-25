
const CWD = process.cwd()
const siteConfig = require(`${CWD}/siteConfig.js`);

var finalhandler = require('finalhandler')
var http = require('http')
var serveStatic = require('serve-static')

var options = {
  index: ['index.html', 'index.htm'],
  extensions: ['html']
}
var serve = serveStatic(`./build/${siteConfig.projectName}`, options)

// Create server
var server = http.createServer(function onRequest (req, res) {
  serve(req, res, finalhandler(req, res))
})

// Listen
server.listen(3000)
