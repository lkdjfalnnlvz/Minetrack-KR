const { Pool } = require('pg');
const logger = require('./logger');

class Database {
  constructor(app) {
    this._app = app;
    const connectionString = process.env.DATABASE_URL;

    if (!connectionString) {
      logger.error('FATAL: DATABASE_URL environment variable is not set!');
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

  // 앱의 다른 부분에서 DB 작업을 할 수 있도록 query 메소드를 제공합니다.
  async query(text, params) {
    return this._pool.query(text, params);
  }
}

module.exports = Database;
