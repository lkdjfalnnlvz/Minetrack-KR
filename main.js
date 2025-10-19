const App = require('./lib/app');
const ServerRegistration = require('./lib/servers');
const logger = require('./lib/logger');
const config = require('./config');
const servers = require('./servers');

// 잠시 기다리는 간단한 함수
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

const main = async () => {
  const app = new App();

  if (config.logToDatabase) {
    logger.info('Database logging is enabled. Initializing database connection...');
    
    // app.loadDatabase()가 new Database()를 호출하므로, 먼저 인스턴스를 만듭니다.
    app.database = new (require('./lib/database'))(app);

    const maxRetries = 10; // 재시도 횟수를 넉넉하게 10번으로 늘립니다.
    for (let i = 1; i <= maxRetries; i++) {
      try {
        // DB 로드 로직을 실행하기 전에, 연결 테스트를 먼저 수행합니다.
        await app.database.testConnection(); // 연결 테스트!
        
        logger.info('Database connection successful! Loading data...');
        await app.database.loadDatabaseAndCleanup(); // 데이터 로드
        break; // 성공하면 루프를 빠져나갑니다.
      } catch (err) {
        logger.error(`Database connection attempt ${i} failed. Retrying in 15 seconds... (Error: ${err.code})`);
        if (i === maxRetries) {
          logger.error('Could not connect to the database after several retries. Exiting.');
          process.exit(1); // 최종 실패 시 앱 종료
        }
        await delay(15000); // 15초씩 기다립니다.
      }
    }
  } else {
      logger.warn('Database logging is not enabled.');
  }
  
  // 서버 정보 초기화 및 시작 로직
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
    logger.warn('"serverGraphDuration" is not defined in config.json - defaulting to 3 minutes!');
    config.serverGraphDuration = 3 * 60 * 10000;
  }

  const port = process.env.PORT || config.site.port;
  config.site.port = parseInt(port, 10);

  app.handleReady();
};

main();
