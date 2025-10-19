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

(async () => {
  if (!config.logToDatabase) {
    logger.log('warn', 'Database logging is not enabled.');
    // handleReady에 콜백을 추가하여 서버가 준비되면 로그를 남깁니다.
    app.handleReady(() => {
      logger.info('Application is running and listening for connections (DB logging disabled).');
    });
  } else {
    await app.loadDatabase();
    // handleReady에 콜백을 추가하여 서버가 준비되면 로그를 남깁니다.
    app.handleReady(() => {
      logger.info('Application is running and listening for connections.');
    });
  }
})();
