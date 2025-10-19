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
      ssl: { rejectUnauthorized: false },
      max: 7
    });

    this._pool.on('error', (err) => {
      logger.error('Unexpected error on idle client in pool', err);
    });
  }

  async ensureIndexes() {
    try {
      await this._pool.query('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip VARCHAR(255), playerCount INTEGER)');
      await this._pool.query('CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255) NOT NULL PRIMARY KEY, playerCount INTEGER)');
      await this._pool.query('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip)');
      await this._pool.query('CREATE INDEX IF NOT EXISTS timestamp_index ON pings (timestamp)');
      logger.info('Database tables and indexes are ready.');
    } catch (err) {
      logger.error('Cannot create table or table index', err);
      throw err;
    }
  }

  async loadDatabaseAndCleanup() {
    await this.ensureIndexes();
    await this.loadGraphPoints(config.graphDuration);
    await this.loadRecords();
    if (config.oldPingsCleanup && config.oldPingsCleanup.enabled) {
      this.initOldPingsDelete();
    }
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
        
        await this._pool.query(
          'INSERT INTO players_record (timestamp, ip, playerCount) VALUES ($1, $2, $3) ON CONFLICT (ip) DO NOTHING',
          [newTimestamp, serverRegistration.data.ip, newPlayerCount]
        );
      }
    });
    await Promise.all(promises);
  }

  async getRecentPings(startTime, endTime) {
    try {
      const result = await this._pool.query('SELECT * FROM pings WHERE timestamp >= $1 AND timestamp <= $2', [startTime, endTime]);
      return result.rows;
    } catch (err) {
      logger.error('Cannot get recent pings', err);
      throw err;
    }
  }

  async getRecord(ip) {
    try {
      const result = await this._pool.query('SELECT playercount, timestamp FROM players_record WHERE ip = $1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get ping record for ${ip}`, err);
      throw err;
    }
  }

  async getRecordLegacy(ip) {
    try {
      const result = await this._pool.query('SELECT playercount, timestamp FROM pings WHERE ip = $1 AND playercount IS NOT NULL ORDER BY playercount DESC LIMIT 1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get legacy ping record for ${ip}`, err);
      throw err;
    }
  }

  async insertPing(ip, timestamp, unsafePlayerCount) {
    try {
      await this._pool.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount]);
    } catch (err) {
      logger.error(`Cannot insert ping record of ${ip} at ${timestamp}`, err);
    }
  }

  async updatePlayerCountRecord(ip, playerCount, timestamp) {
    try {
      await this._pool.query('UPDATE players_record SET timestamp = $1, playerCount = $2 WHERE ip = $3', [timestamp, playerCount, ip]);
    } catch (err) {
      logger.error(`Cannot update player count record of ${ip} at ${timestamp}`, err);
    }
  }

  initOldPingsDelete() {
    this.deleteOldPings();
    const interval = config.oldPingsCleanup.interval || 3600000;
    if (interval > 0) {
      setInterval(() => this.deleteOldPings(), interval);
    }
  }

  async deleteOldPings() {
    const oldestTimestamp = TimeTracker.getEpochMillis() - config.graphDuration;
    const deleteStart = TimeTracker.getEpochMillis();
    try {
      const result = await this._pool.query('DELETE FROM pings WHERE timestamp < $1;', [oldestTimestamp]);
      const deleteTook = TimeTracker.getEpochMillis() - deleteStart;
      if (result.rowCount > 0) {
        logger.info(`Old pings deleted (${result.rowCount} rows) in ${deleteTook}ms`);
      }
    } catch (err) {
      logger.error('Cannot delete old pings', err);
    }
  }
}

module.exports = Database;
