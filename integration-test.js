'use strict';

const request = require('supertest');
const assert = require('assert');
const crypto = require('crypto');
const app = require('./app.js');
const nodeClient = require('./nodeClient.js');

const binaryParser = (res, callback) => {
  const chunks = [];
  res.on('data', (chunk) => chunks.push(chunk));
  res.on('end', () => callback(null, Buffer.concat(chunks)));
};

async function runIntegrationTest() {
  console.log('--- Starting Erasure Storage API Integration Tests ---\n');

  // Reset any down nodes first
  for (let i = 0; i < nodeClient.TOTAL_NODES; i++) {
    await nodeClient.recoverNode(i);
  }

  // 1. Upload a sample file
  console.log('Step 1: Uploading sample file via POST /upload...');
  const sampleData = Buffer.from(
    'Reed-Solomon Integration Test Content: Testing fault-tolerant distributed storage with k=4, m=2 on 6 simulated storage nodes! ' +
    crypto.randomBytes(64).toString('hex'),
    'utf8'
  );
  const fileName = 'test-document.txt';

  const uploadRes = await request(app)
    .post('/upload')
    .attach('file', sampleData, fileName)
    .expect(201);

  assert(uploadRes.body && uploadRes.body.objectId, 'Response must contain objectId');
  const objectId = uploadRes.body.objectId;
  console.log(`  ✓ File uploaded successfully! objectId: ${objectId}\n`);

  // 2. Verify metadata
  console.log('Step 2: Checking GET /objects...');
  const objectsRes = await request(app)
    .get('/objects')
    .expect(200);

  const found = objectsRes.body.find((item) => item.objectId === objectId);
  assert(found, 'Uploaded object must be listed in /objects');
  assert.strictEqual(found.originalName, fileName);
  console.log(`  ✓ Object metadata confirmed in /objects: ${JSON.stringify(found)}\n`);

  // 3. Simulate 2 node failures (e.g. node 0 and node 3)
  console.log('Step 3: Simulating 2 node failures (Node 0 and Node 3)...');
  await request(app)
    .post('/simulate-failure/0')
    .expect(200);

  await request(app)
    .post('/simulate-failure/3')
    .expect(200);

  assert.strictEqual(nodeClient.isNodeAlive(0), false, 'Node 0 should be DOWN');
  assert.strictEqual(nodeClient.isNodeAlive(3), false, 'Node 3 should be DOWN');
  assert.strictEqual(nodeClient.isNodeAlive(1), true, 'Node 1 should be UP');
  assert.strictEqual(nodeClient.isNodeAlive(2), true, 'Node 2 should be UP');
  assert.strictEqual(nodeClient.isNodeAlive(4), true, 'Node 4 should be UP');
  assert.strictEqual(nodeClient.isNodeAlive(5), true, 'Node 5 should be UP');
  console.log('  ✓ Nodes 0 and 3 are DOWN. 4 nodes remain alive (minimum required for k=4).\n');

  // 4. Download file with 2 node failures and verify data integrity
  console.log('Step 4: Downloading file via GET /download/:objectId with 2 failed nodes...');
  const downloadRes = await request(app)
    .get(`/download/${objectId}`)
    .buffer()
    .parse(binaryParser)
    .expect(200);

  assert.deepStrictEqual(
    downloadRes.body,
    sampleData,
    'Downloaded bytes must exactly match the original sample bytes'
  );
  console.log('  ✓ File successfully reconstructed despite 2 dead nodes! Data matches 100%.\n');

  // 5. Simulate a 3rd node failure (now only 3 nodes alive, below threshold of 4)
  console.log('Step 5: Simulating 3rd node failure (Node 5) -> total 3 failed nodes...');
  await request(app)
    .post('/simulate-failure/5')
    .expect(200);

  const failDownloadRes = await request(app)
    .get(`/download/${objectId}`)
    .expect(500);

  assert(
    failDownloadRes.body.error &&
    failDownloadRes.body.error.includes('Not enough shards to reconstruct — too many node failures'),
    'Should return 500 with exact error message when >= 3 nodes fail'
  );
  console.log(`  ✓ Download correctly rejected with 500 when < 4 shards available: "${failDownloadRes.body.error}"\n`);

  // 6. Recover node 0 and download again
  console.log('Step 6: Recovering Node 0 (now 4 nodes alive: 0, 1, 2, 4)...');
  await request(app)
    .post('/recover/0')
    .expect(200);

  const recoveredDownloadRes = await request(app)
    .get(`/download/${objectId}`)
    .buffer()
    .parse(binaryParser)
    .expect(200);

  assert.deepStrictEqual(
    recoveredDownloadRes.body,
    sampleData,
    'Downloaded bytes after recovery must match original'
  );
  console.log('  ✓ Successfully downloaded file after recovering node!\n');

  // Cleanup: Recover all remaining nodes
  await nodeClient.recoverNode(3);
  await nodeClient.recoverNode(5);
  console.log('  ✓ All nodes restored to healthy state.\n');

  console.log('====================================================');
  console.log('  Integration Tests Completed Successfully!         ');
  console.log('====================================================');
}

runIntegrationTest().catch((err) => {
  console.error('Integration test failed:', err);
  process.exit(1);
});
