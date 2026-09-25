'use strict';

const app = require('./app.js');
const { startBackgroundRepair } = require('./repair.js');

const PORT = process.env.PORT || 3000;

const server = app.listen(PORT, () => {
  console.log(`Erasure Storage API server running on port ${PORT}`);
  startBackgroundRepair(15000);
});

module.exports = server;
