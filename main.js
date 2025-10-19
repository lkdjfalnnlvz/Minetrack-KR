const App = require('./lib/app')
const ServerRegistration = require('./lib/servers')
const logger = require('./lib/logger')
const config = require('./config')
const servers = require('./servers')

const main = async () => {
  const app = new App()

  if (config.logToDatabase) {
    try {
      await app.loadDatabase()
    } catch (err) {
      logger.error('Failed to initialize database. Exiting.', err)
      process.exit(1)
    }
  } else {
    logger.warn('Database logging is not enabled.')
  }

  servers.forEach((server, serverId) => {
    if (!server.color) {
      let hash = 0
      for (let i = server.name.length - 1; i >= 0; i--) {
        hash = server.name.charCodeAt(i) + ((hash << 5) - hash)
      }
      const color = Math.floor(Math.abs((Math.sin(hash) * 10000) % 1 * 16777216)).toString(16)
      server.color = '#' + Array(6 - color.length + 1).join('0') + color
    }
    app.serverRegistrations.push(new ServerRegistration(app, serverId, server))
  })

  if (!config.serverGraphDuration) {
    logger.warn('"serverGraphDuration" is not defined in config.json - defaulting to 3 minutes!')
    config.serverGraphDuration = 3 * 60 * 10000
  }

  const port = process.env.PORT || config.site.port
  config.site.port = parseInt(port, 10)

  app.handleReady()
}

main()
