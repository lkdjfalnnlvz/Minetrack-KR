const { Pool } = require('pg');
const logger = require('./logger');
const config = require('../config');
const { TimeTracker } = require('./time');

class Database {
  constructor(app) {
    this._app = app;
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      logger.error('FATAL: DATABASE_URL is not set!');
      process.exit(1);
    }
    this._pool = new Pool({
      connectionString: connectionString,
      ssl: { rejectUnauthorized: false }
    });
    this._pool.on('error', (err) => {
      logger.error('Unexpected error on idle client in pool', err);
    });
  }

  // --- 제가 삭제했던 모든 필수 함수들을 여기에 다시 복원합니다 ---

  async query(text, params) {
    return this._pool.query(text, params);
  }

  async ensureIndexes() {
    try {
      await this.query('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip VARCHAR(255), playerCount INTEGER)');
      await this.query('CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255) NOT NULL PRIMARY KEY, playerCount INTEGER)');
      await this.query('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip)');
      await this.query('CREATE INDEX IF NOT EXISTS timestamp_index ON pings (timestamp)');
      logger.info('Database tables and indexes are ready.');
    } catch (err) {
      logger.error('Cannot create table or table index', err);
      throw err;
    }
  }

  // app.js가 호출하는 바로 그 함수입니다.
  async loadDatabaseAndCleanup() {
    await this.ensureIndexes();
    await this.loadGraphPoints(config.graphDuration);
    await this.loadRecords();
    if (config.oldPingsCleanup && config.oldPingsCleanup.enabled) {
      this.initOldPingsDelete();
    }
  }

  async insertPing(ip, timestamp, unsafePlayerCount) {
    await this.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount]);
  }

  async getRecentPings(startTime, endTime) {
    const { rows } = await this.query('SELECT * FROM pings WHERE timestamp >= $1 AND timestamp <= $2', [startTime, endTime]);
    return rows;
  }
  
  async getRecord(ip) {
    const { rows } = await this.query('SELECT playercount, timestamp FROM players_record WHERE ip = $1', [ip]);
    return rows[0];
  }
  
  async updatePlayerCountRecord(ip, playerCount, timestamp) {
    await this.query('UPDATE players_record SET timestamp = $1, playerCount = $2 WHERE ip = $3', [timestamp, playerCount, ip]);
  }

  async getRecordLegacy(ip) {
    const { rows } = await this.query('SELECT playercount, timestamp FROM pings WHERE ip = $1 AND playercount IS NOT NULL ORDER BY playercount DESC LIMIT 1', [ip]);
    return rows[0];
  }

  async loadGraphPoints(graphDuration) {
    const endTime = TimeTracker.getEpochMillis();
    const startTime = endTime - graphDuration;
    const pingData = await this.getRecentPings(startTime, endTime);
    const relativeGraphData = {};

    for (const row of pingData) {
      let graphData = relativeGraphData[row.ip];
      if (!graphData) {
        relativeGraphData[row.ip] = graphData = [[], []];
      }
      graphData[0].push(row.timestamp);
      graphData[1].push(row.playercount);
    }

    Object.keys(relativeGraphData).forEach(ip => {
      for (const serverRegistration of this._app.serverRegistrations) {
        if (serverRegistration.data.ip === ip) {
          const graphData = relativeGraphData[ip];
          serverRegistration.loadGraphPoints(startTime, graphData[0], graphData[1]);
          break;
        }
      }
    });

    if (Object.keys(relativeGraphData).length > 0) {
      const serverIp = Object.keys(relativeGraphData)[0];
      const timestamps = relativeGraphData[serverIp][0];
      this._app.timeTracker.loadGraphPoints(startTime, timestamps);
    }
  }

  async loadRecords() {
    const promises = this._app.serverRegistrations.map(async (serverRegistration) => {
      serverRegistration.findNewGraphPeak();
      const record = await this.getRecord(serverRegistration.data.ip);

      if (record) {
        serverRegistration.recordData = {
          playerCount: record.playercount,
          timestamp: TimeTracker.toSeconds(record.timestamp)
        };
      } else {
        const legacyRecord = await this.getRecordLegacy(serverRegistration.data.ip);
        let newTimestamp = null;
        let newPlayerCount = null;

        if (legacyRecord) {
          newTimestamp = legacyRecord.timestamp;
          newPlayerCount = legacyRecord.playercount;
        }

        serverRegistration.recordData = {
          playerCount: newPlayerCount,
          timestamp: TimeTracker.toSeconds(newTimestamp)
        };
        
        await this.query(
          'INSERT INTO players_record (timestamp, ip, playerCount) VALUES ($1, $2, $3) ON CONFLICT (ip) DO NOTHING',
          [newTimestamp, serverRegistration.data.ip, newPlayerCount]
        );
      }
    });
    await Promise.all(promises);
  }

  async deleteOldPings() {
    const oldestTimestamp = TimeTracker.getEpochMillis() - config.graphDuration;
    await this.query('DELETE FROM pings WHERE timestamp < $1;', [oldestTimestamp]);
  }

  initOldPingsDelete() {
    this.deleteOldPings();
    const interval = config.oldPingsCleanup.interval || 3600000;
    if (interval > 0) {
      setInterval(() => this.deleteOldPings(), interval);
    }
  }
}

module.exports = Database;
