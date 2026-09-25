'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

const BASE_URL = 'http://localhost:3000';

function postMultipart(endpoint, fieldName, filename, fileBuffer, extraFields = {}) {
  return new Promise((resolve, reject) => {
    const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
    let body = Buffer.alloc(0);

    // Extra fields
    for (const [key, value] of Object.entries(extraFields)) {
      const fieldHeader = Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n${typeof value === 'object' ? JSON.stringify(value) : value}\r\n`
      );
      body = Buffer.concat([body, fieldHeader]);
    }

    // File field
    const fileHeader = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${fieldName}"; filename="${filename}"\r\nContent-Type: application/octet-stream\r\n\r\n`
    );
    const fileFooter = Buffer.from(`\r\n--${boundary}--\r\n`);
    body = Buffer.concat([body, fileHeader, fileBuffer, fileFooter]);

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
          const resBody = Buffer.concat(chunks).toString('utf8');
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: resBody
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

async function runDurabilityPolicyTests() {
  console.log('========================================================================');
  console.log('       CONFIGURABLE DURABILITY POLICIES (k, m) VERIFICATION SUITE       ');
  console.log('========================================================================\n');

  // Step 0: Ensure all baseline nodes 0..8 are recovered/healthy
  for (let i = 0; i < 9; i++) {
    await postRequest(`/recover/${i}`);
  }

  // Step 1: Upload File A with default durability policy (k=4, m=2)
  console.log('Step 1: Uploading File A with DEFAULT policy (k=4, m=2)...');
  const bufferA = Buffer.from('Default Durability File Data: ' + crypto.randomBytes(512).toString('hex'));
  const uploadResA = await postMultipart('/upload', 'file', 'file-default-k4m2.txt', bufferA);
  
  if (uploadResA.statusCode !== 201) {
    throw new Error(`Upload File A failed with status ${uploadResA.statusCode}: ${uploadResA.body}`);
  }
  const metaA = JSON.parse(uploadResA.body);
  const objectIdA = metaA.objectId;
  console.log(`  ✓ File A uploaded (objectId: ${objectIdA})`);

  // Verify metadata on disk for File A
  const diskMetaA = JSON.parse(fs.readFileSync(path.join(__dirname, 'metadata', `${objectIdA}.json`), 'utf8'));
  if (diskMetaA.k !== 4 || diskMetaA.m !== 2 || diskMetaA.checksums.length !== 6) {
    throw new Error(`Expected File A metadata to have k=4, m=2, 6 checksums; got k=${diskMetaA.k}, m=${diskMetaA.m}, len=${diskMetaA.checksums.length}`);
  }
  console.log(`  ✓ Verified File A metadata: k=${diskMetaA.k}, m=${diskMetaA.m}, totalShards=${diskMetaA.checksums.length}`);

  // Step 2: Upload File B with CUSTOM durability policy (k=6, m=3) -> 9 shards total
  console.log('\nStep 2: Uploading File B with CUSTOM policy (k=6, m=3 -> 9 total shards)...');
  const bufferB = Buffer.from('Custom Durability File Data: ' + crypto.randomBytes(1024).toString('hex'));
  const uploadResB = await postMultipart('/upload', 'file', 'file-custom-k6m3.txt', bufferB, {
    durabilityPolicy: { k: 6, m: 3 }
  });

  if (uploadResB.statusCode !== 201) {
    throw new Error(`Upload File B failed with status ${uploadResB.statusCode}: ${uploadResB.body}`);
  }
  const metaB = JSON.parse(uploadResB.body);
  const objectIdB = metaB.objectId;
  console.log(`  ✓ File B uploaded (objectId: ${objectIdB})`);

  // Verify metadata on disk for File B
  const diskMetaB = JSON.parse(fs.readFileSync(path.join(__dirname, 'metadata', `${objectIdB}.json`), 'utf8'));
  if (diskMetaB.k !== 6 || diskMetaB.m !== 3 || diskMetaB.checksums.length !== 9) {
    throw new Error(`Expected File B metadata to have k=6, m=3, 9 checksums; got k=${diskMetaB.k}, m=${diskMetaB.m}, len=${diskMetaB.checksums.length}`);
  }
  console.log(`  ✓ Verified File B metadata: k=${diskMetaB.k}, m=${diskMetaB.m}, totalShards=${diskMetaB.checksums.length}`);

  // Confirm dynamic nodes 6, 7, 8 were created and shards exist
  for (let i = 0; i < 9; i++) {
    const shardFile = path.join(__dirname, 'nodes', `node${i}`, `${objectIdB}.shard`);
    if (!fs.existsSync(shardFile)) {
      throw new Error(`Missing expected shard for File B on dynamically scaled node${i}`);
    }
  }
  console.log('  ✓ Verified all 9 shards written across dynamic storage nodes node0 through node8.');

  // Step 3: Test 2 Node Failures (Node 0 & Node 1 DOWN)
  console.log('\nStep 3: Simulating 2 node failures (Node 0 & Node 1 DOWN)...');
  await postRequest('/simulate-failure/0');
  await postRequest('/simulate-failure/1');

  // Both File A (4 of 6 alive >= 4) and File B (7 of 9 alive >= 6) should succeed
  const dlResA_2down = await getBuffer(`/download/${objectIdA}`);
  if (dlResA_2down.statusCode !== 200 || !dlResA_2down.buffer.equals(bufferA)) {
    throw new Error('File A failed to reconstruct with 2 node failures');
  }
  console.log('  ✓ File A (k=4, m=2) survives 2 failures! 100% byte match.');

  const dlResB_2down = await getBuffer(`/download/${objectIdB}`);
  if (dlResB_2down.statusCode !== 200 || !dlResB_2down.buffer.equals(bufferB)) {
    throw new Error('File B failed to reconstruct with 2 node failures');
  }
  console.log('  ✓ File B (k=6, m=3) survives 2 failures! 100% byte match.');

  // Step 4: Test 3 Node Failures (Node 0, Node 1, Node 2 DOWN)
  console.log('\nStep 4: Simulating 3rd node failure (Node 2 DOWN) -> Total 3 dead nodes...');
  await postRequest('/simulate-failure/2');

  // File A (k=4) only has 3 alive shards (< 4) -> MUST FAIL WITH 500
  const dlResA_3down = await getBuffer(`/download/${objectIdA}`);
  if (dlResA_3down.statusCode !== 500) {
    throw new Error(`Expected File A (k=4) to fail with 3 dead nodes, but got status ${dlResA_3down.statusCode}`);
  }
  console.log('  ✓ File A (k=4, m=2) correctly FAILS with 3 node failures (insufficient shards < 4).');

  // File B (k=6) has 6 alive shards (nodes 3,4,5,6,7,8) == k -> MUST SURVIVE & SUCCEED!
  const dlResB_3down = await getBuffer(`/download/${objectIdB}`);
  if (dlResB_3down.statusCode !== 200 || !dlResB_3down.buffer.equals(bufferB)) {
    throw new Error('File B (k=6, m=3) failed to reconstruct with 3 node failures');
  }
  console.log('  ✓ File B (k=6, m=3) SURVIVES 3 node failures! (Operating at minimum k=6 threshold). 100% byte match.');

  // Step 5: Test 4 Node Failures for File B (Node 3 DOWN -> Total 4 dead nodes: 0, 1, 2, 3)
  console.log('\nStep 5: Simulating 4th node failure (Node 3 DOWN) -> Total 4 dead nodes...');
  await postRequest('/simulate-failure/3');

  // File B (k=6) now has 5 alive shards (< 6) -> MUST FAIL WITH 500
  const dlResB_4down = await getBuffer(`/download/${objectIdB}`);
  if (dlResB_4down.statusCode !== 500) {
    throw new Error(`Expected File B (k=6) to fail with 4 dead nodes, but got status ${dlResB_4down.statusCode}`);
  }
  console.log('  ✓ File B (k=6, m=3) correctly FAILS with 4 node failures (insufficient shards < 6).');

  // Step 6: Recovery and Final Verification
  console.log('\nStep 6: Recovering all nodes and performing full post-recovery verification...');
  for (let i = 0; i < 9; i++) {
    await postRequest(`/recover/${i}`);
  }

  const dlResA_rec = await getBuffer(`/download/${objectIdA}`);
  const dlResB_rec = await getBuffer(`/download/${objectIdB}`);

  if (dlResA_rec.statusCode !== 200 || !dlResA_rec.buffer.equals(bufferA)) {
    throw new Error('File A failed post-recovery verification');
  }
  if (dlResB_rec.statusCode !== 200 || !dlResB_rec.buffer.equals(bufferB)) {
    throw new Error('File B failed post-recovery verification');
  }
  console.log('  ✓ All nodes restored to healthy state. Both objects download cleanly.');

  console.log('\n========================================================================');
  console.log('           ALL DURABILITY POLICY TESTS PASSED SUCCESSFULLY!             ');
  console.log('========================================================================\n');
}

runDurabilityPolicyTests().catch((err) => {
  console.error('\n❌ Durability policy test suite failed:', err);
  process.exit(1);
});
