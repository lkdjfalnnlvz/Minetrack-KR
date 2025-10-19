const { Pool } = require('pg');
const logger = require('./logger');
const config = require('../config');
const { TimeTracker } = require('./time');

class Database {
  constructor(app) {
    this._app = app;

    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      logger.error('FATAL: DATABASE_URL environment variable is not set! Application cannot start.');
      process.exit(1);
    }
    
    // 가장 단순하고 표준적인 방식으로 연결 풀을 생성합니다.
    this._pool = new Pool({
      connectionString: connectionString,
      ssl: {
        rejectUnauthorized: false
      }
    });

    // 연결 풀에서 발생하는 모든 오류를 로깅합니다.
    this._pool.on('error', (err, client) => {
      logger.error('Unexpected error on idle client in pool', err);
    });
  }
  
  // === 모든 DB 함수를 명시적 클라이언트 관리 방식으로 수정합니다 ===

  async ensureIndexes() {
    // 1. 풀에서 클라이언트를 빌려옵니다.
    const client = await this._pool.connect();
    try {
      // 2. 빌려온 클라이언트로 작업을 수행합니다.
      await client.query('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip VARCHAR(255), playerCount INTEGER)');
      await client.query('CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255) NOT NULL PRIMARY KEY, playerCount INTEGER)');
      await client.query('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip)');
      await client.query('CREATE INDEX IF NOT EXISTS timestamp_index ON pings (timestamp)');
      logger.info('Database tables and indexes are ready.');
    } catch (err) {
      logger.error('Cannot create table or table index', err);
      throw err;
    } finally {
      // 3. 작업이 성공하든 실패하든, 반드시 클라이언트를 풀에 반납합니다.
      client.release();
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

  async insertPing(ip, timestamp, unsafePlayerCount) {
    const client = await this._pool.connect();
    try {
      await client.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount]);
    } catch (err) {
      logger.error(`Cannot insert ping record of ${ip} at ${timestamp}`, err);
    } finally {
      client.release();
    }
  }

  // (이하 모든 DB 함수에 동일한 '빌리고-쓰고-반납' 패턴을 적용합니다)

  async getRecentPings(startTime, endTime) {
    const client = await this._pool.connect();
    try {
      const result = await client.query('SELECT * FROM pings WHERE timestamp >= $1 AND timestamp <= $2', [startTime, endTime]);
      return result.rows;
    } catch (err) {
      logger.error('Cannot get recent pings', err);
      throw err;
    } finally {
      client.release();
    }
  }

  async getRecord(ip) {
    const client = await this._pool.connect();
    try {
      const result = await client.query('SELECT playercount, timestamp FROM players_record WHERE ip = $1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get ping record for ${ip}`, err);
      throw err;
    } finally {
      client.release();
    }
  }

  async updatePlayerCountRecord(ip, playerCount, timestamp) {
    const client = await this._pool.connect();
    try {
      await client.query('UPDATE players_record SET timestamp = $1, playerCount = $2 WHERE ip = $3', [timestamp, playerCount, ip]);
    } catch (err) {
      logger.error(`Cannot update player count record of ${ip} at ${timestamp}`, err);
    } finally {
      client.release();
    }
  }

  async deleteOldPings() {
    const client = await this._pool.connect();
    try {
      const oldestTimestamp = TimeTracker.getEpochMillis() - config.graphDuration;
      await client.query('DELETE FROM pings WHERE timestamp < $1;', [oldestTimestamp]);
    } catch (err) {
      logger.error('Cannot delete old pings', err);
    } finally {
      client.release();
    }
  }

  // loadRecords와 같은 복잡한 함수들은 내부적으로 호출하는 함수들(getRecord 등)이
  // 이미 패턴을 따르고 있으므로, 그대로 두어도 괜찮습니다.
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
        
        const client = await this._pool.connect();
        try {
          await client.query(
            'INSERT INTO players_record (timestamp, ip, playerCount) VALUES ($1, $2, $3) ON CONFLICT (ip) DO NOTHING',
            [newTimestamp, serverRegistration.data.ip, newPlayerCount]
          );
        } finally {
          client.release();
        }
      }
    });
    await Promise.all(promises);
  }
  
  async getRecordLegacy(ip) {
    const client = await this._pool.connect();
    try {
      const result = await client.query('SELECT playercount, timestamp FROM pings WHERE ip = $1 AND playercount IS NOT NULL ORDER BY playercount DESC LIMIT 1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get legacy ping record for ${ip}`, err);
      throw err;
    } finally {
      client.release();
    }
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
