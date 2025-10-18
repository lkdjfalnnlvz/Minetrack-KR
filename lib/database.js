// lib/database.js 파일의 최종 수정본 (이 코드로 전체를 교체하세요)

const { Pool } = require('pg');
const logger = require('./logger');
const config = require('../config');
const { TimeTracker } = require('./time');

class Database {
  constructor(app) {
    this._app = app;
    // PostgreSQL 연결 풀(Pool) 생성
    this._pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false
      }
      family: 4
    });
  }

  // 데이터베이스 테이블과 인덱스를 생성/확인하는 함수
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

  // 앱 시작 시 데이터베이스를 초기화하는 전체 로직
  async loadDatabaseAndCleanup() {
    await this.ensureIndexes();
    await this.loadGraphPoints(config.graphDuration);
    await this.loadRecords();
    if (config.oldPingsCleanup && config.oldPingsCleanup.enabled) {
      this.initOldPingsDelete();
    }
  }

  // 그래프 데이터를 DB에서 불러오는 함수
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
      graphData[1].push(row.playercount); // PostgreSQL은 컬럼명을 소문자로 반환
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

  // 최고 기록을 DB에서 불러오는 함수
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

  // 최근 핑 기록을 가져오는 함수
  async getRecentPings(startTime, endTime) {
    try {
      const result = await this._pool.query('SELECT * FROM pings WHERE timestamp >= $1 AND timestamp <= $2', [startTime, endTime]);
      return result.rows;
    } catch (err) {
      logger.error('Cannot get recent pings', err);
      throw err;
    }
  }

  // 특정 서버의 최고 기록을 가져오는 함수
  async getRecord(ip) {
    try {
      const result = await this._pool.query('SELECT playercount, timestamp FROM players_record WHERE ip = $1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get ping record for ${ip}`, err);
      throw err;
    }
  }

  // 레거시 최고 기록을 가져오는 함수 (초기 마이그레이션용)
  async getRecordLegacy(ip) {
    try {
      const result = await this._pool.query('SELECT playercount, timestamp FROM pings WHERE ip = $1 AND playercount IS NOT NULL ORDER BY playercount DESC LIMIT 1', [ip]);
      return result.rows[0];
    } catch (err) {
      logger.error(`Cannot get legacy ping record for ${ip}`, err);
      throw err;
    }
  }

  // 핑 데이터를 DB에 삽입하는 함수
  async insertPing(ip, timestamp, unsafePlayerCount) {
    try {
      await this._pool.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount]);
    } catch (err) {
      logger.error(`Cannot insert ping record of ${ip} at ${timestamp}`, err);
    }
  }

  // 최고 기록을 DB에 업데이트하는 함수
  async updatePlayerCountRecord(ip, playerCount, timestamp) {
    try {
      await this._pool.query('UPDATE players_record SET timestamp = $1, playerCount = $2 WHERE ip = $3', [timestamp, playerCount, ip]);
    } catch (err) {
      logger.error(`Cannot update player count record of ${ip} at ${timestamp}`, err);
    }
  }

  // 오래된 핑 데이터를 주기적으로 삭제하는 로직을 초기화하는 함수
  initOldPingsDelete() {
    this.deleteOldPings();
    const interval = config.oldPingsCleanup.interval || 3600000;
    if (interval > 0) {
      setInterval(() => this.deleteOldPings(), interval);
    }
  }

  // 오래된 핑 데이터를 삭제하는 함수
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
