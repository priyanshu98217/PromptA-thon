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

async function runCorruptionTests() {
  console.log('--- Starting Reed-Solomon Data Corruption & Integrity Tests ---\n');

  // Reset all nodes to UP
  for (let i = 0; i < nodeClient.TOTAL_NODES; i++) {
    await nodeClient.recoverNode(i);
  }

  // Step 1: Upload test file with checksums
  console.log('Step 1: Uploading sample file to generate checksummed shards across nodes 0..5...');
  const originalPayload = Buffer.from(
    'INTEGRITY VERIFICATION TEST: Reed-Solomon SHA-256 Checksum Validation & Bit-Rot Recovery! ' +
    crypto.randomBytes(128).toString('hex'),
    'utf8'
  );
  const fileName = 'integrity-doc.txt';

  const uploadRes = await request(app)
    .post('/upload')
    .attach('file', originalPayload, fileName)
    .expect(201);

  const objectId = uploadRes.body.objectId;
  assert(objectId, 'Upload should return objectId');
  console.log(`  ✓ Uploaded objectId: ${objectId}\n`);

  // Verify metadata contains checksums array
  const metadata = await nodeClient.getMetadata(objectId);
  assert(Array.isArray(metadata.checksums), 'Metadata must contain checksums array');
  assert.strictEqual(metadata.checksums.length, 6, 'Must contain 6 checksums');
  for (let i = 0; i < 6; i++) {
    assert.strictEqual(typeof metadata.checksums[i], 'string', `Checksum for node ${i} must be string`);
    assert.strictEqual(metadata.checksums[i].length, 64, `Checksum for node ${i} must be 64-char SHA256 hex`);
  }
  console.log(`  ✓ Verified SHA-256 checksums stored in metadata: ${metadata.checksums[0].slice(0, 10)}...\n`);

  // Step 2: Corrupt 1 shard on disk (Node 0)
  console.log('Step 2: Corrupting shard on Node 0 via POST /corrupt-shard/0/:objectId...');
  await request(app)
    .post(`/corrupt-shard/0/${objectId}`)
    .expect(200);

  // Assert Node 0 is STILL ALIVE (folder exists, node is online)
  assert.strictEqual(nodeClient.isNodeAlive(0), true, 'Node 0 must still be online (alive)');
  console.log('  ✓ Node 0 remains ONLINE/ALIVE, but its shard file has been modified/corrupted.');

  // Step 3: Download with 1 corrupted shard -> Must reconstruct 100% matching original
  console.log('Step 3: Downloading with 1 corrupted shard (Node 0 corrupted, Nodes 1..5 valid)...');
  const download1Res = await request(app)
    .get(`/download/${objectId}`)
    .buffer()
    .parse(binaryParser)
    .expect(200);

  assert.deepStrictEqual(
    download1Res.body,
    originalPayload,
    'Reconstructed bytes with 1 corrupted shard must match original payload 100%'
  );
  console.log('  ✓ Reconstructed file with 1 corrupted shard! Bit-rot detected & bypassed successfully.\n');

  // Step 4: Corrupt 2nd shard on disk (Node 4 - a parity shard)
  console.log('Step 4: Corrupting 2nd shard (Node 4 - parity shard) -> Total 2 corrupted shards...');
  await request(app)
    .post(`/corrupt-shard/4/${objectId}`)
    .expect(200);

  assert.strictEqual(nodeClient.isNodeAlive(4), true, 'Node 4 must still be online');

  // Step 5: Download with 2 corrupted shards (Node 0 and Node 4 corrupted, Nodes 1, 2, 3, 5 valid)
  console.log('Step 5: Downloading with 2 corrupted shards (4 valid shards remaining: 1, 2, 3, 5)...');
  const download2Res = await request(app)
    .get(`/download/${objectId}`)
    .buffer()
    .parse(binaryParser)
    .expect(200);

  assert.deepStrictEqual(
    download2Res.body,
    originalPayload,
    'Reconstructed bytes with 2 corrupted shards must match original payload 100%'
  );
  console.log('  ✓ Reconstructed file with 2 corrupted shards! (Operating at minimum threshold of 4 valid shards).\n');

  // Step 6: Test hybrid failure (1 node DOWN + 1 shard CORRUPTED = 2 lost shards, 4 valid shards)
  console.log('Step 6: Testing hybrid failure: Node 1 is taken DOWN (simulated crash) + Node 0 is CORRUPTED...');
  // Restore node 4 shard first
  const { shards } = await require('./erasure.js').encodeBuffer(originalPayload);
  await nodeClient.writeShard(4, objectId, shards[4].data);

  // Take Node 1 DOWN
  await request(app).post('/simulate-failure/1').expect(200);
  assert.strictEqual(nodeClient.isNodeAlive(1), false, 'Node 1 is DOWN');
  assert.strictEqual(nodeClient.isNodeAlive(0), true, 'Node 0 is ONLINE (but shard is corrupted)');

  const downloadHybridRes = await request(app)
    .get(`/download/${objectId}`)
    .buffer()
    .parse(binaryParser)
    .expect(200);

  assert.deepStrictEqual(
    downloadHybridRes.body,
    originalPayload,
    'Hybrid scenario (1 dead node + 1 corrupted shard) must reconstruct original bytes'
  );
  console.log('  ✓ Successfully reconstructed file under hybrid failure (1 node DOWN + 1 shard CORRUPTED)!\n');

  // Step 7: Corrupt a 3rd shard (Total 3 unusable shards: 1 dead node + 2 corrupted shards -> only 3 valid shards)
  console.log('Step 7: Corrupting another shard (Node 2) so 3 shards are unusable -> Total 3 valid shards remaining (< 4)...');
  await request(app).post(`/corrupt-shard/2/${objectId}`).expect(200);

  const failRes = await request(app)
    .get(`/download/${objectId}`)
    .expect(500);

  assert(
    failRes.body.error && failRes.body.error.includes('Not enough shards to reconstruct'),
    'Should return 500 when fewer than 4 valid/uncorrupted shards remain'
  );
  console.log(`  ✓ Correctly rejected download with 500 when 3 shards are unusable: "${failRes.body.error}"\n`);

  // Cleanup: Recover node 1 and restore shards
  await nodeClient.recoverNode(1);
  for (const shard of shards) {
    await nodeClient.writeShard(shard.index, objectId, shard.data);
  }
  console.log('  ✓ Restored all nodes and shards to clean healthy state.\n');

  console.log('====================================================');
  console.log('  All Corruption & Integrity Tests Passed!          ');
  console.log('====================================================');
}

runCorruptionTests().catch((err) => {
  console.error('Corruption test failed:', err);
  process.exit(1);
});
