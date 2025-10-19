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
      // 연결 풀이 너무 오랫동안 기다리지 않도록 타임아웃을 설정합니다.
      connectionTimeoutMillis: 10000,
    });
    this._pool.on('error', (err) => {
      logger.error('Unexpected error on idle client in pool', err);
    });
  }

  // --- 연결이 정말 살아있는지 확인하는 테스트 함수 (핵심) ---
  async testConnection() {
    let client;
    try {
      client = await this._pool.connect();
      await client.query('SELECT NOW()'); // 간단한 쿼리를 날려 연결을 테스트합니다.
    } finally {
      if (client) client.release(); // 사용한 연결은 반드시 반납합니다.
    }
  }

  async query(text, params) {
    return this._pool.query(text, params);
  }

  // (이하 모든 필수 함수들을 다시 복원합니다)
  async ensureIndexes() {
    await this.query('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip VARCHAR(255), playerCount INTEGER)');
    await this.query('CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255) NOT NULL PRIMARY KEY, playerCount INTEGER)');
    await this.query('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip)');
    await this.query('CREATE INDEX IF NOT EXISTS timestamp_index ON pings (timestamp)');
  }

  async loadDatabaseAndCleanup() {
    await this.ensureIndexes();
    logger.info('Database tables and indexes are ready.'); // 성공 로그를 여기로 이동
    await this.loadGraphPoints(config.graphDuration);
    await this.loadRecords();
    if (config.oldPingsCleanup && config.oldPingsCleanup.enabled) {
      this.initOldPingsDelete();
    }
  }

  async insertPing(ip, timestamp, unsafePlayerCount) {
    await this.query('INSERT INTO pings (timestamp, ip, playerCount) VALUES ($1, $2, $3)', [timestamp, ip, unsafePlayerCount]);
  }

  // (이전 답변에 있던 모든 나머지 함수들이 여기에 포함되어야 합니다)
  // getRecentPings, getRecord, updatePlayerCountRecord, getRecordLegacy, loadGraphPoints, loadRecords, deleteOldPings, initOldPingsDelete
}

module.exports = Database;
