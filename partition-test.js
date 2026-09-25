'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

const BASE_URL = 'http://localhost:3000';

function postMultipart(endpoint, fieldName, filename, fileBuffer, additionalFields = {}) {
  return new Promise((resolve, reject) => {
    const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
    const chunks = [];

    for (const [key, value] of Object.entries(additionalFields)) {
      chunks.push(Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n${value}\r\n`
      ));
    }

    chunks.push(Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${fieldName}"; filename="${filename}"\r\nContent-Type: application/octet-stream\r\n\r\n`
    ));
    chunks.push(fileBuffer);
    chunks.push(Buffer.from(`\r\n--${boundary}--\r\n`));

    const body = Buffer.concat(chunks);
    const url = new URL(endpoint, BASE_URL);

    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname + (url.search || ''),
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': body.length
        }
      },
      (res) => {
        const resChunks = [];
        res.on('data', (c) => resChunks.push(c));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            body: Buffer.concat(resChunks).toString('utf8')
          });
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function postJson(endpoint, jsonPayload = {}) {
  return new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(jsonPayload), 'utf8');
    const url = new URL(endpoint, BASE_URL);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname + (url.search || ''),
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
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

function getJson(endpoint) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint, BASE_URL);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname + (url.search || ''),
        method: 'GET'
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try {
            resolve({
              statusCode: res.statusCode,
              data: JSON.parse(Buffer.concat(chunks).toString('utf8'))
            });
          } catch (e) {
            resolve({
              statusCode: res.statusCode,
              raw: Buffer.concat(chunks).toString('utf8')
            });
          }
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
        path: url.pathname + (url.search || ''),
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

async function runPartitionTests() {
  console.log('========================================================================');
  console.log('        NETWORK PARTITION & REACHABILITY SIMULATION TEST SUITE          ');
  console.log('========================================================================\n');

  // Step 0: Ensure all base nodes are recovered and partition is healed
  await postJson('/heal-partition');
  for (let i = 0; i < 6; i++) {
    await postJson(`/recover/${i}`);
  }

  // Step 1: Upload a file with standard k=4, m=2 policy
  console.log('Step 1: Uploading test payload (k=4, m=2 across 6 storage nodes)...');
  const originalBuffer = Buffer.from('NETWORK PARTITION TEST PAYLOAD: ' + crypto.randomBytes(32768).toString('hex'));
  const originalChecksum = crypto.createHash('sha256').update(originalBuffer).digest('hex');

  const uploadRes = await postMultipart('/upload', 'file', 'partition-test-file.bin', originalBuffer);
  if (uploadRes.statusCode !== 201) {
    throw new Error(`Upload failed: ${uploadRes.body}`);
  }
  const { objectId } = JSON.parse(uploadRes.body);
  console.log(`  ✓ Upload succeeded (objectId: ${objectId}, originalSize: ${originalBuffer.length} bytes)`);

  // Step 2: Simulate partition where 4 nodes are reachable (Group A: [0,1,2,3], Group B: [4,5])
  console.log('\nStep 2: Simulating network partition (Reachable Group A: [0, 1, 2, 3], Partitioned Group B: [4, 5])...');
  const partRes1 = await postJson('/simulate-partition', {
    groupA: [0, 1, 2, 3],
    groupB: [4, 5],
    activeGroup: 'groupA'
  });
  if (partRes1.statusCode !== 200) {
    throw new Error(`Simulate partition failed: ${partRes1.body}`);
  }
  console.log('  ✓ Partition simulated successfully.');

  // Verify node status under partition
  const statusRes1 = await getJson(`/nodes/status?objectId=${objectId}`);
  const statusData1 = statusRes1.data;
  if (!statusData1.partitionState || !statusData1.partitionState.active) {
    throw new Error('Expected partitionState.active to be true');
  }
  console.log(`  ✓ Cluster status under partition: ${statusData1.healthStatus} (${statusData1.validCount}/6 reachable valid shards)`);
  
  // Verify disk state: folders MUST NOT be renamed
  const node4Dir = path.join(__dirname, 'nodes', 'node4');
  const node5Dir = path.join(__dirname, 'nodes', 'node5');
  if (!fs.existsSync(node4Dir) || !fs.existsSync(node5Dir)) {
    throw new Error('Partitioned nodes must remain intact on disk without folder renaming');
  }
  console.log('  ✓ Verified nodes/node4 and nodes/node5 folders remain unrenamed (node process healthy, only network unreachable).');

  // Attempt download: should succeed because 4 valid shards >= k=4
  console.log('  • Attempting download with 4 reachable nodes (operating at minimum k=4 threshold)...');
  const dlRes1 = await getBuffer(`/download/${objectId}`);
  if (dlRes1.statusCode !== 200) {
    throw new Error(`Download failed with 4 reachable nodes: HTTP ${dlRes1.statusCode}`);
  }
  if (!dlRes1.buffer.equals(originalBuffer)) {
    throw new Error('Downloaded buffer does not match original buffer under 4-node partition!');
  }
  console.log('  ✓ Download succeeded! 100% byte match despite 2 partitioned nodes.');

  // Step 3: Simulate partition where only 3 nodes are reachable (Group A: [0,1,2], Group B: [3,4,5])
  console.log('\nStep 3: Simulating severe network partition (Reachable Group A: [0, 1, 2], Partitioned Group B: [3, 4, 5])...');
  const partRes2 = await postJson('/simulate-partition', {
    groupA: [0, 1, 2],
    groupB: [3, 4, 5],
    activeGroup: 'groupA'
  });
  if (partRes2.statusCode !== 200) {
    throw new Error(`Simulate partition failed: ${partRes2.body}`);
  }

  const statusRes2 = await getJson(`/nodes/status?objectId=${objectId}`);
  const statusData2 = statusRes2.data;
  console.log(`  ✓ Cluster status under severe partition: ${statusData2.healthStatus} (${statusData2.validCount}/6 reachable shards)`);
  if (statusData2.healthStatus !== 'IMPOSSIBLE') {
    throw new Error(`Expected healthStatus IMPOSSIBLE when reachable shards < 4, got: ${statusData2.healthStatus}`);
  }

  // Attempt download: should fail with HTTP 500 (3 < k=4)
  console.log('  • Attempting download with only 3 reachable nodes (3 < k=4)...');
  const dlRes2 = await getBuffer(`/download/${objectId}`);
  if (dlRes2.statusCode === 200) {
    throw new Error('Expected download to fail when fewer than k=4 shards are reachable!');
  }
  console.log(`  ✓ Download correctly REJECTED with HTTP ${dlRes2.statusCode} (insufficient reachable shards).`);

  // Step 4: Attempt write during severe partition
  console.log('\nStep 4: Attempting upload during severe partition (only 3 nodes reachable)...');
  const newUploadBuffer = Buffer.from('ANOTHER FILE WHILE PARTITIONED');
  const uploadRes2 = await postMultipart('/upload', 'file', 'partition-fail-file.bin', newUploadBuffer);
  if (uploadRes2.statusCode === 201) {
    throw new Error('Expected upload to fail when fewer than k=4 nodes are reachable to accept writes');
  }
  console.log(`  ✓ Write operation correctly REJECTED with HTTP ${uploadRes2.statusCode} (fewer than k nodes reachable).`);

  // Step 5: Heal network partition
  console.log('\nStep 5: Healing network partition via POST /heal-partition...');
  const healRes = await postJson('/heal-partition');
  if (healRes.statusCode !== 200) {
    throw new Error(`Heal partition failed: ${healRes.body}`);
  }
  console.log('  ✓ Partition healed. Restored full cluster reachability.');

  const statusRes3 = await getJson(`/nodes/status?objectId=${objectId}`);
  const statusData3 = statusRes3.data;
  console.log(`  ✓ Post-heal cluster status: ${statusData3.healthStatus} (${statusData3.validCount}/6 reachable valid shards)`);
  if (statusData3.validCount !== 6 || statusData3.healthStatus !== 'HEALTHY') {
    throw new Error(`Expected all 6 nodes valid & HEALTHY after heal, got validCount=${statusData3.validCount}`);
  }

  // Step 6: Verify full download after healing
  console.log('\nStep 6: Verifying download and reconstruction after healing partition...');
  const dlRes3 = await getBuffer(`/download/${objectId}`);
  if (dlRes3.statusCode !== 200) {
    throw new Error(`Failed to download object after partition heal: HTTP ${dlRes3.statusCode}`);
  }
  if (!dlRes3.buffer.equals(originalBuffer)) {
    throw new Error('Downloaded bytes do not match original payload after healing partition!');
  }
  const downloadedChecksum = crypto.createHash('sha256').update(dlRes3.buffer).digest('hex');
  if (downloadedChecksum !== originalChecksum) {
    throw new Error('Checksum mismatch after healing partition!');
  }
  console.log(`  ✓ Object downloaded successfully with 100% byte-for-byte integrity (SHA-256: ${downloadedChecksum.slice(0, 16)}...)!`);

  console.log('\n========================================================================');
  console.log('         ALL NETWORK PARTITION TESTS PASSED SUCCESSFULLY!               ');
  console.log('========================================================================\n');
}

runPartitionTests().catch((err) => {
  console.error('\n❌ Network partition test failed:', err);
  process.exit(1);
});
