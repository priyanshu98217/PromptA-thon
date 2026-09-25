'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const nodeClient = require('./nodeClient.js');

const BASE_URL = 'http://localhost:3000';

function postMultipart(endpoint, fieldName, filename, fileBuffer, extraFields = {}) {
  return new Promise((resolve, reject) => {
    const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
    let body = Buffer.alloc(0);

    for (const [key, value] of Object.entries(extraFields)) {
      const fieldHeader = Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n${typeof value === 'object' ? JSON.stringify(value) : value}\r\n`
      );
      body = Buffer.concat([body, fieldHeader]);
    }

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
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
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

function postRequest(endpoint, bodyObj = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint, BASE_URL);
    const payload = bodyObj ? JSON.stringify(bodyObj) : null;
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST',
        headers: payload ? {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload)
        } : {}
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
    if (payload) req.write(payload);
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
        path: url.pathname + url.search,
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

async function runVersioningTests() {
  console.log('========================================================================');
  console.log('       OBJECT VERSIONING & STALE WRITE CONSISTENCY TEST SUITE          ');
  console.log('========================================================================\n');

  // Step 0: Ensure cluster healthy
  for (let i = 0; i < 6; i++) {
    await postRequest(`/recover/${i}`);
  }

  // Step 1: Upload Version 1
  console.log('Step 1: Uploading Version 1 of the object...');
  const bufferV1 = Buffer.from('--- VERSION 1 CONTENT --- ' + crypto.randomBytes(512).toString('hex'));
  const uploadRes1 = await postMultipart('/upload', 'file', 'document.txt', bufferV1);

  if (uploadRes1.statusCode !== 201) {
    throw new Error(`Upload v1 failed: ${uploadRes1.body}`);
  }
  const meta1 = JSON.parse(uploadRes1.body);
  const objectId = meta1.objectId;
  console.log(`  ✓ Version 1 uploaded (objectId: ${objectId}, version: ${meta1.version})`);

  // Verify on-disk version 1 files
  for (let i = 0; i < 6; i++) {
    const v1File = path.join(__dirname, 'nodes', `node${i}`, `${objectId}_v1.shard`);
    if (!fs.existsSync(v1File)) {
      throw new Error(`Expected v1 shard on node${i}`);
    }
  }
  console.log('  ✓ Verified version-tagged files <objectId>_v1.shard written across all nodes.');

  // Step 2: Upload Version 2 to the same objectId
  console.log('\nStep 2: Uploading Version 2 to the SAME objectId...');
  const bufferV2 = Buffer.from('--- VERSION 2 CONTENT (UPDATED) --- ' + crypto.randomBytes(1024).toString('hex'));
  const uploadRes2 = await postMultipart('/upload', 'file', 'document-v2.txt', bufferV2, {
    objectId: objectId
  });

  if (uploadRes2.statusCode !== 201) {
    throw new Error(`Upload v2 failed: ${uploadRes2.body}`);
  }
  const meta2 = JSON.parse(uploadRes2.body);
  if (meta2.version !== 2 || meta2.objectId !== objectId) {
    throw new Error(`Expected version 2 for objectId ${objectId}, got: ${JSON.stringify(meta2)}`);
  }
  console.log(`  ✓ Version 2 uploaded successfully (objectId: ${objectId}, version: ${meta2.version})`);

  // Verify metadata history
  const diskMeta = await nodeClient.getMetadata(objectId);
  if (!diskMeta.versions || diskMeta.versions.length !== 2 || diskMeta.currentVersion !== 2) {
    throw new Error(`Invalid version history in metadata: ${JSON.stringify(diskMeta.versions)}`);
  }
  console.log(`  ✓ Verified metadata version history: currentVersion=${diskMeta.currentVersion}, totalVersions=${diskMeta.versions.length}`);

  // Confirm both v1 and v2 shards coexist on disk
  for (let i = 0; i < 6; i++) {
    const v1Path = path.join(__dirname, 'nodes', `node${i}`, `${objectId}_v1.shard`);
    const v2Path = path.join(__dirname, 'nodes', `node${i}`, `${objectId}_v2.shard`);
    if (!fs.existsSync(v1Path) || !fs.existsSync(v2Path)) {
      throw new Error(`Expected both _v1 and _v2 shard files on node${i}`);
    }
  }
  console.log('  ✓ Coexistence verified: both _v1 and _v2 shards exist side-by-side without overwriting.');

  // Step 3: Verify downloading both versions
  console.log('\nStep 3: Verifying downloads of both versions before failure injection...');
  
  // Default download should give v2
  const dlLatest = await getBuffer(`/download/${objectId}`);
  if (dlLatest.statusCode !== 200 || !dlLatest.buffer.equals(bufferV2)) {
    throw new Error('Default download did not return latest version (v2)');
  }
  console.log('  ✓ Default GET /download/:objectId returned Version 2 (100% byte match).');

  // Explicit ?version=1
  const dlV1 = await getBuffer(`/download/${objectId}?version=1`);
  if (dlV1.statusCode !== 200 || !dlV1.buffer.equals(bufferV1)) {
    throw new Error('GET /download/:objectId?version=1 did not return Version 1');
  }
  console.log('  ✓ GET /download/:objectId?version=1 returned Version 1 (100% byte match).');

  // Explicit ?version=2
  const dlV2 = await getBuffer(`/download/${objectId}?version=2`);
  if (dlV2.statusCode !== 200 || !dlV2.buffer.equals(bufferV2)) {
    throw new Error('GET /download/:objectId?version=2 did not return Version 2');
  }
  console.log('  ✓ GET /download/:objectId?version=2 returned Version 2 (100% byte match).');

  // Step 4: Simulate Stale Write on Node 0 (Node 0 missed the update, only has v1)
  console.log('\nStep 4: Simulating stale write inconsistency on Node 0 (Node 0 missed v2 update)...');
  const staleRes = await postRequest(`/simulate-stale-write/0/${objectId}`, { staleVersion: 1 });
  if (staleRes.statusCode !== 200) {
    throw new Error(`Failed to simulate stale write: ${staleRes.body}`);
  }
  console.log('  ✓ Node 0 is ONLINE, but its shard for v2 is MISSING (stale v1 shard remains).');

  // Step 5: Download latest version with 1 stale node (Nodes 1..5 have v2, Node 0 is stale)
  console.log('\nStep 5: Downloading latest version with stale node present...');
  const dlLatestWithStale = await getBuffer(`/download/${objectId}`);
  if (dlLatestWithStale.statusCode !== 200 || !dlLatestWithStale.buffer.equals(bufferV2)) {
    throw new Error('Failed to reconstruct latest version in presence of stale shard');
  }
  console.log('  ✓ Latest version reconstructed successfully! Stale v1 shard on Node 0 was bypassed.');

  // Step 6: Verify Version 1 download still succeeds
  console.log('\nStep 6: Verifying Version 1 download with stale node...');
  const dlV1StillWorks = await getBuffer(`/download/${objectId}?version=1`);
  if (dlV1StillWorks.statusCode !== 200 || !dlV1StillWorks.buffer.equals(bufferV1)) {
    throw new Error('Failed to download v1');
  }
  console.log('  ✓ Historical Version 1 remains intact and downloadable!');

  // Step 7: Simulate stale writes on 2 more nodes (Nodes 1 and 2 stale -> total 3 stale nodes for v2)
  console.log('\nStep 7: Simulating 3 stale nodes for v2 (Nodes 0, 1, 2 only have v1, leaving 3 valid v2 shards < 4)...');
  await postRequest(`/simulate-stale-write/1/${objectId}`, { staleVersion: 1 });
  await postRequest(`/simulate-stale-write/2/${objectId}`, { staleVersion: 1 });

  const dlTooManyStale = await getBuffer(`/download/${objectId}`);
  if (dlTooManyStale.statusCode !== 500) {
    throw new Error(`Expected 500 when < 4 valid shards of v2 exist, but got status ${dlTooManyStale.statusCode}`);
  }
  console.log('  ✓ Download of latest version correctly REJECTED with 500 when < 4 current-version shards exist.');

  // Step 8: Version 1 still has all 6 shards available -> should still succeed!
  const dlV1With3StaleV2 = await getBuffer(`/download/${objectId}?version=1`);
  if (dlV1With3StaleV2.statusCode !== 200 || !dlV1With3StaleV2.buffer.equals(bufferV1)) {
    throw new Error('Historical v1 failed to reconstruct');
  }
  console.log('  ✓ Historical Version 1 still reconstructs cleanly with 6/6 shards available!');

  console.log('\n========================================================================');
  console.log('          ALL OBJECT VERSIONING TESTS PASSED SUCCESSFULLY!               ');
  console.log('========================================================================\n');
}

runVersioningTests().catch((err) => {
  console.error('\n❌ Versioning test suite failed:', err);
  process.exit(1);
});
