// lib/database.js 파일의 새 내용 (이 코드로 전체를 교체하세요)

const { Pool } = require('pg') // 변경: sqlite3 대신 pg (node-postgres) 라이브러리를 사용합니다.
const logger = require('./logger')
const config = require('../config')
const { TimeTracker } = require('./time')

class Database {
  constructor (app) {
    this._app = app
    // 변경: PostgreSQL 연결 풀(Pool)을 생성합니다.
    // 연결 정보는 Render의 환경 변수(DATABASE_URL)에서 자동으로 가져옵니다.
    this._pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false
      }
    })
  }

  // 참고: getDailyDatabase 기능은 파일 시스템에 의존하므로 Render와 같은 클라우드 환경에서는
  // 적합하지 않아 제거했습니다. PostgreSQL에서는 데이터베이스 자체가 영구적으로 데이터를 보관합니다.

  async ensureIndexes () {
    // 변경: async/await를 사용하고, SQL 문법을 PostgreSQL에 맞게 수정합니다 (TINYTEXT -> VARCHAR, MEDIUMINT -> INTEGER).
    // serialize 없이 순차적으로 실행됩니다.
    try {
      await this._pool.query('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip VARCHAR(255), playerCount INTEGER)')
      await this._pool.query('CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255) NOT NULL PRIMARY KEY, playerCount INTEGER)')
      await this._pool.query('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip, playerCount)')
      await this._pool.query('CREATE INDEX IF NOT EXISTS timestamp_index ON pings (timestamp)')
      logger.info('Database tables and indexes are ready.')
    } catch (err) {
      logger.error('Cannot create table or table index', err)
      throw err
    }
  }

  // 참고: 기존의 콜백(callback) 기반 메소드들을 async/await를 사용하도록 전체적으로 리팩토링했습니다.
  // app.js에서 이 메소드들을 호출하는 부분도 수정이 필요할 수 있습니다. (아래 4단계에서 안내)
  async loadDatabase () {
    await this.ensureIndexes()
    await this.loadGraphPoints(config.graphDuration)
    await this.loadRecords()
    if (config.oldPingsCleanup && config.oldPingsCleanup.enabled) {
      this.initOldPingsDelete()
    }
  }

  async loadGraphPoints (graphDuration) {
    const endTime = TimeTracker.getEpochMillis()
    const startTime = endTime - graphDuration

    const pingData = await this.getRecentPings(startTime, endTime)
    const relativeGraphData = {}

    for (const row of pingData) {
      let graphData = relativeGraphData[row.ip]
      if (!graphData) {
        relativeGraphData[row.ip] = graphData = [[], []]
      }
      graphData[0].push(row.timestamp)
      graphData[1].push(row.playercount) // PostgreSQL은 컬럼명을 소문자로 반환합니다.
    }

    Object.keys(relativeGraphData).forEach(ip => {
      for (const serverRegistration of this._app.serverRegistrations) {
        if (serverRegistration.data.ip === ip) {
          const graphData = relativeGraphData[ip]
          serverRegistration.loadGraphPoints(startTime, graphData[0], graphData[1])
          break
        }
      }
    })

    if (Object.keys(relativeGraphData).length > 0) {
      const serverIp = Object.keys(relativeGraphData)[0]
      const timestamps = relativeGraphData[serverIp][0]
      this._app.timeTracker.loadGraphPoints(startTime, timestamps)
    }
  }

  async loadRecords () {
    const promises = this._app.serverRegistrations.map(async (serverRegistration) => {
      serverRegistration.findNewGraphPeak()

      let record = await this.getRecord(serverRegistration.data.ip)

      if (record) {
        serverRegistration.recordData = {
          playerCount: record.playercount,
          timestamp: TimeTracker.toSeconds(record.timestamp)
        }
      } else {
        const legacyRecord = await this.getRecordLegacy(serverRegistration.data.ip)
        let newTimestamp = null
        let newPlayerCount = null

        if (legacyRecord) {
          newTimestamp = legacyRecord.timestamp
          newPlayerCount = legacyRecord.playercount
        }

        serverRegistration.recordData = {
          playerCount: newPlayerCount,
          timestamp: TimeTracker.toSeconds(newTimestamp)
        }
        
        await this._pool.query('INSERT INTO players_record (timestamp, ip, playerCount) VALUES ($1, $2, $3) ON CONFLICT (ip) DO NOTHING', 
          [newTimestamp, serverRegistration.data.ip, newPlayerCount]);
      }
    })
    await Promise.all(promises)
  }

  async getRecentPings (startTime, endTime) {
    try {
      const result = await this._pool.query('SELECT * FROM pings WHERE timestamp >= $1 AND timestamp <= $2', [startTime, endTime])
      return result.rows
    } catch (err) {
      logger.error('Cannot get recent pings', err)
      throw err
    }
  }

  async getRecord (ip) {
    try {
      const result = await this._pool.query('SELECT playercount, timestamp FROM players_record WHERE ip = $1', [ip])
      return result.rows[0] // Record가 없으면 undefined 반환
    } catch (err) {
      logger.error(`Cannot get ping record for ${ip}`, err)
      throw err
    }
  }

  async getRecordLegacy (ip) {
    try {
      const result = await this._pool.query('SELECT MAX(playerCount) as playercount, timestamp FROM pings WHERE ip = $1 GROUP BY timestamp', [ip])
      const record = result.rows[0];
      if (record && record.playercount !== null) {
          return record;
      }
      return null;
    } catch (err) {
      logger.error(`Cannot get legacy ping record for ${ip}`, err)
      throw err
    }
  }

  async insertPing (ip, timestamp, unsafePlayerCount) {
    try {
      await this._pool.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount])
    } catch (err) {
      logger.error(`Cannot insert ping record of ${ip} at ${timestamp}`, err)
      throw err
    }
  }

  async updatePlayerCountRecord (ip, playerCount, timestamp) {
    try {
      await this._pool.query('UPDATE players_record SET timestamp = $1, playerCount = $2 WHERE ip = $3', [timestamp, playerCount, ip])
    } catch (err) {
      logger.error(`Cannot update player count record of ${ip} at ${timestamp}`, err)
      throw err
    }
  }

  initOldPingsDelete () {
    logger.info('Deleting old pings..')
    this.deleteOldPings().then(() => {
      const oldPingsCleanupInterval = config.oldPingsCleanup.interval || 3600000
      if (oldPingsCleanupInterval > 0) {
        setInt