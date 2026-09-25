'use strict';

const request = require('supertest');
const assert = require('assert');
const app = require('./app.js');
const nodeClient = require('./nodeClient.js');

async function testDashboardThresholds() {
  console.log('--- Testing Dashboard Node Status Thresholds ---\n');

  // Reset all nodes to UP
  for (let i = 0; i < 6; i++) {
    await nodeClient.recoverNode(i);
  }

  // 1. Initial State: 6 nodes UP -> HEALTHY
  console.log('Test State 1: 6 / 6 nodes online...');
  let res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 6);
  assert.strictEqual(res.body.healthStatus, 'HEALTHY');
  assert.strictEqual(res.body.nodes.filter(n => n.isAlive).length, 6);
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 2. Kill 1 node (Node 0) -> 5 / 6 nodes online -> HEALTHY
  console.log('Test State 2: 5 / 6 nodes online (Kill Node 0)...');
  await request(app).post('/simulate-failure/0').expect(200);
  res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 5);
  assert.strictEqual(res.body.healthStatus, 'HEALTHY');
  assert.strictEqual(res.body.nodes[0].isAlive, false);
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 3. Kill 2nd node (Node 1) -> 4 / 6 nodes online -> DEGRADED
  console.log('Test State 3: 4 / 6 nodes online (Kill Node 1) -> Exactly minimum threshold...');
  await request(app).post('/simulate-failure/1').expect(200);
  res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 4);
  assert.strictEqual(res.body.healthStatus, 'DEGRADED');
  assert.strictEqual(res.body.nodes[0].isAlive, false);
  assert.strictEqual(res.body.nodes[1].isAlive, false);
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 4. Kill 3rd node (Node 2) -> 3 / 6 nodes online -> IMPOSSIBLE
  console.log('Test State 4: 3 / 6 nodes online (Kill Node 2) -> Below minimum threshold...');
  await request(app).post('/simulate-failure/2').expect(200);
  res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 3);
  assert.strictEqual(res.body.healthStatus, 'IMPOSSIBLE');
  assert.strictEqual(res.body.nodes[2].isAlive, false);
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 5. Revive Node 2 -> 4 nodes online -> DEGRADED
  console.log('Test State 5: Reviving Node 2 -> 4 / 6 nodes online...');
  await request(app).post('/recover/2').expect(200);
  res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 4);
  assert.strictEqual(res.body.healthStatus, 'DEGRADED');
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 6. Revive Node 0 & 1 -> 6 nodes online -> HEALTHY
  console.log('Test State 6: Reviving Node 0 & 1 -> 6 / 6 nodes online...');
  await request(app).post('/recover/0').expect(200);
  await request(app).post('/recover/1').expect(200);
  res = await request(app).get('/nodes/status').expect(200);
  assert.strictEqual(res.body.aliveCount, 6);
  assert.strictEqual(res.body.healthStatus, 'HEALTHY');
  console.log(`  ✓ Status: ${res.body.healthStatus} ("${res.body.healthMessage}")\n`);

  // 7. Verify index.html is served statically
  console.log('Test State 7: Verifying GET / serves public/index.html...');
  const htmlRes = await request(app).get('/').expect(200);
  assert(htmlRes.text.includes('Reed-Solomon Erasure Storage'), 'index.html must be served at /');
  console.log('  ✓ Dashboard HTML served successfully!\n');

  console.log('====================================================');
  console.log('  All Dashboard Threshold Tests Passed!             ');
  console.log('====================================================');
}

testDashboardThresholds().catch((err) => {
  console.error('Dashboard test failed:', err);
  process.exit(1);
});
