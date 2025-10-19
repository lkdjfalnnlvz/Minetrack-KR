const Database = require('./database')
const PingController = require('./ping')
const Server = require('./server')
const { TimeTracker } = require('./time')
const MessageOf = require('./message')

const config = require('../config')
const minecraftVersions = require('../minecraft_versions')

class App {
  serverRegistrations = []

  constructor () {
    this.pingController = new PingController(this)
    this.server = new Server(this)
    this.timeTracker = new TimeTracker(this)
  }

  async loadDatabase () {
    this.database = new Database(this)
    await this.database.loadDatabaseAndCleanup()
  }

  handleReady () {
    this.server.listen(config.site.ip, config.site.port)
    this.pingController.schedule()
  }

  handleClientConnection = (client) => {
    if (config.logToDatabase) {
      client.on('message', (message) => {
        if (message === 'requestHistoryGraph') {
          const graphData = this.serverRegistrations.map(serverRegistration => serverRegistration.graphData)

          client.send(MessageOf('historyGraph', {
            timestamps: this.timeTracker.getGraphPoints(),
            graphData
          }))
        }
      })
    }

    const initMessage = {
      config: (() => {
        const minecraftVersionNames = {}
        Object.keys(minecraftVersions).forEach(function (key) {
          minecraftVersionNames[key] = minecraftVersions[key].map(version => version.name)
        })

        return {
          graphDurationLabel: config.graphDurationLabel || (Math.floor(config.graphDuration / (60 * 60 * 1000)) + 'h'),
          graphMaxLength: TimeTracker.getMaxGraphDataLength(),
          serverGraphMaxLength: TimeTracker.getMaxServerGraphDataLength(),
          servers: this.serverRegistrations.map(serverRegistration => serverRegistration.getPublicData()),
          minecraftVersions: minecraftVersionNames,
          isGraphVisible: config.logToDatabase
        }
      })(),
      timestampPoints: this.timeTracker.getServerGraphPoints(),
      servers: this.serverRegistrations.map(serverRegistration => serverRegistration.getPingHistory())
    }

    client.send(MessageOf('init', initMessage))
  }
}

module.exports = App
