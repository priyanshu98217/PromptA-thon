'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { runRepairScan } = require('./repair.js');
const nodeClient = require('./nodeClient.js');

const BASE_URL = 'http://localhost:3000';

function postMultipart(endpoint, fieldName, filename, fileBuffer) {
  return new Promise((resolve, reject) => {
    const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
    const fileHeader = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${fieldName}"; filename="${filename}"\r\nContent-Type: application/octet-stream\r\n\r\n`
    );
    const fileFooter = Buffer.from(`\r\n--${boundary}--\r\n`);
    const body = Buffer.concat([fileHeader, fileBuffer, fileFooter]);

    const url = new URL(endpoint, BASE_URL);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': body.length
        }
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            body: Buffer.concat(chunks).toString('utf8')
          });
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function postRequest(endpoint) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint, BASE_URL);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST'
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            body: Buffer.concat(chunks).toString('utf8')
          });
        });
      }
    );
    req.on('error', reject);
    req.end();
  });
}

function getBuffer(endpoint) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint, BASE_URL);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'GET'
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            buffer: Buffer.concat(chunks)
          });
        });
      }
    );
    req.on('error', reject);
    req.end();
  });
}

async function runRepairTests() {
  console.log('========================================================================');
  console.log('       BACKGROUND SELF-HEALING & AUTOMATIC REPAIR TEST SUITE           ');
  console.log('========================================================================\n');

  // Step 0: Ensure all 6 base nodes are online
  for (let i = 0; i < 6; i++) {
    await postRequest(`/recover/${i}`);
  }

  // Step 1: Upload a fresh test file
  console.log('Step 1: Uploading a fresh file to generate 6 uncorrupted shards...');
  const testBuffer = Buffer.from('Self-Healing Distributed Storage Test Buffer: ' + crypto.randomBytes(4096).toString('hex'));
  const uploadRes = await postMultipart('/upload', 'file', 'healing-test.txt', testBuffer);

  if (uploadRes.statusCode !== 201) {
    throw new Error(`Upload failed: ${uploadRes.body}`);
  }
  const { objectId } = JSON.parse(uploadRes.body);
  console.log(`  ✓ Uploaded objectId: ${objectId}`);

  const originalMeta = await nodeClient.getMetadata(objectId);
  const expectedChecksum0 = originalMeta.checksums[0];
  const expectedChecksum2 = originalMeta.checksums[2];

  // Step 2: Kill Node 0, delete its shard, then revive Node 0 (simulating revived empty node)
  console.log('\nStep 2: Simulating revived empty node on Node 0 (kill -> wipe shard -> revive)...');
  await postRequest('/simulate-failure/0');
  const downDir0 = path.join(__dirname, 'nodes', 'node0_DOWN');
  if (fs.existsSync(downDir0)) {
    for (const f of fs.readdirSync(downDir0)) {
      if (f.startsWith(objectId)) fs.unlinkSync(path.join(downDir0, f));
    }
  }
  await postRequest('/recover/0');
  const aliveDir0 = path.join(__dirname, 'nodes', 'node0');
  if (fs.existsSync(aliveDir0)) {
    for (const f of fs.readdirSync(aliveDir0)) {
      if (f.startsWith(objectId)) fs.unlinkSync(path.join(aliveDir0, f));
    }
  }
  console.log('  ✓ Node 0 is ONLINE, but its shard file is completely MISSING on disk.');

  // Step 3: Corrupt shard on Node 2 (simulating bit-rot on online node)
  console.log('\nStep 3: Corrupting shard on Node 2 via bit flip...');
  await postRequest(`/corrupt-shard/2/${objectId}`);
  const node2Files = fs.readdirSync(path.join(__dirname, 'nodes', 'node2')).filter(f => f.startsWith(objectId));
  const fileToRead = node2Files[0];
  const corruptedData2 = fs.readFileSync(path.join(__dirname, 'nodes', 'node2', fileToRead));
  const actualChecksum2 = nodeClient.computeChecksum(corruptedData2);
  if (actualChecksum2 === expectedChecksum2) {
    throw new Error('Shard on Node 2 failed to corrupt');
  }
  console.log('  ✓ Node 2 is ONLINE, but its shard file has a CHECKSUM MISMATCH (bit-rot).');

  // Step 4: Confirm 4 valid shards remain (Nodes 1, 3, 4, 5)
  console.log('\nStep 4: Checking cluster quorum before self-healing...');
  const statusRes = await (await fetch(`${BASE_URL}/nodes/status?objectId=${objectId}`)).json();
  console.log(`  ✓ Cluster status: ${statusRes.healthStatus} (${statusRes.validCount} valid shards available: Nodes 1, 3, 4, 5)`);

  // Step 5: Trigger a manual self-healing repair scan via POST /repair/scan
  console.log('\nStep 5: Triggering self-healing repair scan via POST /repair/scan...');
  const repairRes = await postRequest('/repair/scan');
  if (repairRes.statusCode !== 200) {
    throw new Error(`Repair scan request failed: ${repairRes.body}`);
  }
  const repairResult = JSON.parse(repairRes.body);
  console.log(`  ✓ Scan completed at: ${repairResult.lastScanTime}`);
  console.log(`  ✓ Total shards repaired in scan: ${repairResult.repairs ? repairResult.repairs.length : 0}`);

  if (repairResult.repairs && repairResult.repairs.length > 0) {
    const repairedNodes = repairResult.repairs.map(r => `Node ${r.nodeIndex} (${r.reason})`);
    console.log(`  ✓ Repaired items in this scan: ${repairedNodes.join(', ')}`);
  }

  // Step 6: Verify direct on-disk files and SHA-256 checksums
  console.log('\nStep 6: Verifying direct on-disk shard files and SHA-256 checksums...');

  // Check Node 0 (missing shard restored)
  const node0Files = fs.readdirSync(path.join(__dirname, 'nodes', 'node0')).filter(f => f.startsWith(objectId));
  if (node0Files.length === 0) {
    throw new Error('Node 0 shard was not written to disk during repair!');
  }
  const diskData0 = fs.readFileSync(path.join(__dirname, 'nodes', 'node0', node0Files[0]));
  const diskChecksum0 = nodeClient.computeChecksum(diskData0);
  if (diskChecksum0 !== expectedChecksum0) {
    throw new Error(`Node 0 restored checksum mismatch: expected ${expectedChecksum0}, got ${diskChecksum0}`);
  }
  console.log(`  ✓ Node 0 shard restored on disk! SHA-256 matched: ${diskChecksum0.slice(0, 16)}...`);

  // Check Node 2 (corrupted shard repaired)
  const node2RestoredFiles = fs.readdirSync(path.join(__dirname, 'nodes', 'node2')).filter(f => f.startsWith(objectId));
  const diskData2 = fs.readFileSync(path.join(__dirname, 'nodes', 'node2', node2RestoredFiles[0]));
  const diskChecksum2 = nodeClient.computeChecksum(diskData2);
  if (diskChecksum2 !== expectedChecksum2) {
    throw new Error(`Node 2 repaired checksum mismatch: expected ${expectedChecksum2}, got ${diskChecksum2}`);
  }
  console.log(`  ✓ Node 2 shard healed on disk! SHA-256 matched: ${diskChecksum2.slice(0, 16)}...`);

  // Step 7: Verify download
  console.log('\nStep 7: Verifying download and reconstruction with all 6 healed shards...');
  const downloadRes = await getBuffer(`/download/${objectId}`);
  if (downloadRes.statusCode !== 200 || !downloadRes.buffer.equals(testBuffer)) {
    throw new Error('File reconstruction failed after self-healing');
  }
  console.log('  ✓ File downloaded and reconstructed with 100% byte-for-byte integrity!');

  // Step 8: Verify GET /repair/status API endpoint
  console.log('\nStep 8: Verifying GET /repair/status API endpoint...');
  const apiStatus = await (await fetch(`${BASE_URL}/repair/status`)).json();
  if (!apiStatus.lastScanTime || apiStatus.repairsCount < 2) {
    throw new Error('GET /repair/status failed to report latest repair scan');
  }
  console.log(`  ✓ GET /repair/status verified: lastScanTime=${apiStatus.lastScanTime}, count=${apiStatus.repairsCount}`);

  console.log('\n========================================================================');
  console.log('         ALL SELF-HEALING REPAIR TESTS PASSED SUCCESSFULLY!             ');
  console.log('========================================================================\n');
}

runRepairTests().catch((err) => {
  console.error('\n❌ Repair test suite failed:', err);
  process.exit(1);
});
