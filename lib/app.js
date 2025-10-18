// lib/app.js 파일의 새 내용 (이 코드로 전체를 교체하세요)

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

  // === 여기를 수정했습니다 ===
  // 콜백(callback) 기반의 복잡한 코드를 async/await를 사용하는 간결한 코드로 변경했습니다.
  async loadDatabase () {
    this.database = new Database(this)
    // database.js에 새로 만든 loadDatabaseAndCleanup 함수를 await로 호출합니다.
    await this.database.loadDatabaseAndCleanup()
  }
  // ==========================

  handleReady (callback) {
    // this.server.listen에 callback을 그대로 전달합니다.
    this.server.listen(config.site.ip, config.site.port, callback);
    this.pingController.schedule();
  }

  handleClientConnection = (client) => {
    if (config.logToDatabase) {
      client.on('message', (message) => {
        if (message === 'requestHistoryGraph') {
          // Send historical graphData built from all serverRegistrations
          const graphData = this.serverRegistrations.map(serverRegistration => serverRegistration.graphData)

          // Send graphData in object wrapper to avoid needing to explicity filter
          // any header data being appended by #MessageOf since the graph data is fed
          // directly into the graphing system
          client.send(MessageOf('historyGraph', {
            timestamps: this.timeTracker.getGraphPoints(),
            graphData
          }))
        }
      })
    }

    const initMessage = {
      config: (() => {
        // Remap minecraftVersion entries into name values
        const minecraftVersionNames = {}
        Object.keys(minecraftVersions).forEach(function (key) {
          minecraftVersionNames[key] = minecraftVersions[key].map(version => version.name)
        })

        // Send configuration data for rendering the page
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
} // <--- 이 중괄호가 실수로 삭제되었을 가능성이 높습니다.

module.exports = App;
