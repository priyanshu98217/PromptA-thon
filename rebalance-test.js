'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

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
        path: url.pathname + (url.search || ''),
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
        path: url.pathname + (url.search || ''),
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

async function runRebalanceTests() {
  console.log('========================================================================');
  console.log('         CLUSTER NODE ADDITION & SHARD REBALANCING TEST SUITE           ');
  console.log('========================================================================\n');

  // Step 0: Ensure base nodes 0..5 are recovered and alive
  for (let i = 0; i < 6; i++) {
    await postRequest(`/recover/${i}`);
  }

  // Step 1: Upload 3 files to initial 6-node cluster
  console.log('Step 1: Uploading 3 distinct files across the initial 6 storage nodes...');
  const files = [
    { name: 'rebalance-file-1.bin', buffer: Buffer.from('FILE 1: ' + crypto.randomBytes(1024).toString('hex')) },
    { name: 'rebalance-file-2.bin', buffer: Buffer.from('FILE 2: ' + crypto.randomBytes(2048).toString('hex')) },
    { name: 'rebalance-file-3.bin', buffer: Buffer.from('FILE 3: ' + crypto.randomBytes(4096).toString('hex')) }
  ];

  for (const f of files) {
    const uploadRes = await postMultipart('/upload', 'file', f.name, f.buffer);
    if (uploadRes.statusCode !== 201) {
      throw new Error(`Upload failed for ${f.name}: ${uploadRes.body}`);
    }
    f.objectId = JSON.parse(uploadRes.body).objectId;
    console.log(`  ✓ Uploaded ${f.name} (objectId: ${f.objectId})`);
  }

  // Step 2: Check initial cluster load
  console.log('\nStep 2: Inspecting initial cluster load via GET /nodes/load...');
  const initialLoadRes = await getJson('/nodes/load');
  const initialLoad = initialLoadRes.data;
  console.log(`  ✓ Cluster has ${initialLoad.totalNodes} nodes, storing ${initialLoad.totalShards} total shards.`);
  for (const n of initialLoad.nodes) {
    console.log(`    • ${n.name}: ${n.shardCount} shards`);
  }

  // Step 3: Register a new storage node joining the cluster
  console.log('\nStep 3: Registering a brand new storage node via POST /nodes/add...');
  const addNodeRes = await postRequest('/nodes/add');
  if (addNodeRes.statusCode !== 201) {
    throw new Error(`Failed to add node: ${addNodeRes.body}`);
  }
  const addNodeData = JSON.parse(addNodeRes.body);
  const newNodeIndex = addNodeData.nodeIndex;
  console.log(`  ✓ ${addNodeData.name} registered and ready (nodeIndex: ${newNodeIndex}).`);

  // Verify new node starts with 0 shards
  const loadWithNewNode = await getJson('/nodes/load');
  const newNodeStat = loadWithNewNode.data.nodes.find(n => n.nodeIndex === newNodeIndex);
  if (!newNodeStat || newNodeStat.shardCount !== 0) {
    throw new Error(`Expected new node ${newNodeIndex} to have 0 shards initially`);
  }
  console.log(`  ✓ Verified ${newNodeStat.name} currently has 0 shards (underloaded).`);

  // Step 4: Trigger Cluster Rebalance
  console.log('\nStep 4: Triggering cluster rebalance via POST /rebalance...');
  const rebalanceRes = await postRequest('/rebalance');
  if (rebalanceRes.statusCode !== 200) {
    throw new Error(`Rebalance request failed: ${rebalanceRes.body}`);
  }
  const rebalanceData = JSON.parse(rebalanceRes.body);
  console.log(`  ✓ Rebalance scan complete: ${rebalanceData.movesCount} shard migration(s) performed.`);
  for (const m of rebalanceData.moves) {
    console.log(`    • Migrated shard ${m.shardIndex} of object ${m.objectId.slice(0, 8)}... from node${m.fromNode} -> node${m.toNode}`);
  }

  if (rebalanceData.movesCount === 0) {
    throw new Error('Expected rebalancer to migrate shards to newly added node');
  }

  // Step 5: Verify load distribution after rebalancing
  console.log('\nStep 5: Verifying cluster load distribution after rebalancing...');
  const postLoadRes = await getJson('/nodes/load');
  const postLoad = postLoadRes.data;
  console.log(`  ✓ Post-rebalance cluster distribution (${postLoad.totalNodes} nodes, ${postLoad.totalShards} total shards):`);
  
  const shardCounts = postLoad.nodes.filter(n => n.isAlive).map(n => n.shardCount);
  for (const n of postLoad.nodes) {
    console.log(`    • ${n.name}: ${n.shardCount} shards`);
  }

  const minShards = Math.min(...shardCounts);
  const maxShards = Math.max(...shardCounts);
  const postNewNodeStat = postLoad.nodes.find(n => n.nodeIndex === newNodeIndex);
  if (!postNewNodeStat || postNewNodeStat.shardCount === 0) {
    throw new Error(`New node ${newNodeIndex} should store shards after rebalance`);
  }
  console.log(`  ✓ Confirmed ${postNewNodeStat.name} now stores ${postNewNodeStat.shardCount} shards! (Successfully adopted shards from overloaded nodes)`);

  // Step 6: Verify all objects download and reconstruct byte-identically
  console.log('\nStep 6: Verifying all objects are 100% downloadable and byte-identical after migration...');
  for (const f of files) {
    const dlRes = await getBuffer(`/download/${f.objectId}`);
    if (dlRes.statusCode !== 200) {
      throw new Error(`Failed to download ${f.name} after rebalance: HTTP ${dlRes.statusCode}`);
    }
    if (!dlRes.buffer.equals(f.buffer)) {
      throw new Error(`Downloaded content for ${f.name} does not match original bytes after rebalance!`);
    }
    console.log(`  ✓ Reconstructed ${f.name} (objectId: ${f.objectId}) with 100% byte-for-byte integrity!`);
  }

  console.log('\n========================================================================');
  console.log('         ALL CLUSTER REBALANCING TESTS PASSED SUCCESSFULLY!             ');
  console.log('========================================================================\n');
}

runRebalanceTests().catch((err) => {
  console.error('\n❌ Rebalance test suite failed:', err);
  process.exit(1);
});
