const { URL } = require('url');
const dns = require('dns').promises; // DNS 조회를 위한 모듈
const App = require('./lib/app');
const ServerRegistration = require('./lib/servers');
const logger = require('./lib/logger');
const config = require('./config');
const servers = require('./servers');

const app = new App();

servers.forEach((server, serverId) => {
  if (!server.color) {
    let hash = 0;
    for (let i = server.name.length - 1; i >= 0; i--) {
      hash = server.name.charCodeAt(i) + ((hash << 5) - hash);
    }
    const color = Math.floor(Math.abs((Math.sin(hash) * 10000) % 1 * 16777216)).toString(16);
    server.color = '#' + Array(6 - color.length + 1).join('0') + color;
  }
  app.serverRegistrations.push(new ServerRegistration(app, serverId, server));
});

if (!config.serverGraphDuration) {
  logger.log('warn', '"serverGraphDuration" is not defined in config.json - defaulting to 3 minutes!');
  config.serverGraphDuration = 3 * 60 * 10000;
}

const port = process.env.PORT || config.site.port;
config.site.port = parseInt(port, 10);

// === 여기가 핵심 수정 부분입니다 ===
(async () => {
  // 데이터베이스 로깅이 활성화되어 있고, DATABASE_URL이 설정된 경우에만 실행
  if (config.logToDatabase && process.env.DATABASE_URL) {
    try {
      logger.info('Resolving database hostname to IPv4 address...');
      const dbUrl = new URL(process.env.DATABASE_URL);
      const { address } = await dns.lookup(dbUrl.hostname, { family: 4 });

      logger.info(`Resolved ${dbUrl.hostname} to ${address}. Updating connection string.`);
      
      // 기존 주소의 호스트네임 부분을 조회된 IPv4 주소로 교체
      dbUrl.hostname = address;
      
      // 변경된 주소를 환경 변수에 덮어쓰기하여 이후 모든 DB 연결이 이 주소를 사용하도록 함
      process.env.DATABASE_URL = dbUrl.toString();
    } catch (err) {
      logger.error('Failed to resolve database hostname to IPv4. Proceeding with original URL.', err);
    }
  }

  // 데이터베이스 로드 및 서버 시작 로직은 동일
  if (!config.logToDatabase) {
    logger.log('warn', 'Database logging is not enabled.');
    app.handleReady(() => {
      logger.info('Application is running and listening for connections (DB logging disabled).');
    });
  } else {
    await app.loadDatabase();
    app.handleReady(() => {
      logger.info('Application is running and listening for connections.');
    });
  }
})();
