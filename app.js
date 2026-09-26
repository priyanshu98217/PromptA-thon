'use strict';

const path = require('path');
const express = require('express');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const mime = require('mime-types');
const { encodeBuffer, decodeShards } = require('./erasure.js');
const nodeClient = require('./nodeClient.js');
const repair = require('./repair.js');
const rebalance = require('./rebalance.js');

const app = express();
app.use(express.json());

// Serve static assets from public/ directory
app.use(express.static(path.join(__dirname, 'public')));

// Explicit route to serve the Erasure Storage web dashboard UI at '/'
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/**
 * GET /nodes/status
 * Returns alive/down state, network partition status, corruption status, and overall health of storage nodes.
 * Accepts optional query param `?objectId=...` to check shard integrity against a specific object.
 */
app.get('/nodes/status', async (req, res) => {
  try {
    const objectId = req.query.objectId;
    let targetMetadata = null;
    if (objectId) {
      targetMetadata = await nodeClient.getMetadata(objectId);
    } else {
      const allObjects = await nodeClient.listAllMetadata();
      if (allObjects.length > 0) {
        targetMetadata = await nodeClient.getMetadata(allObjects[0].objectId);
      }
    }

    const k = targetMetadata ? (targetMetadata.k || 4) : 4;
    const m = targetMetadata ? (targetMetadata.m || 2) : 2;
    const totalNodesCount = Math.max(nodeClient.TOTAL_NODES, k + m);

    const nodes = [];
    let aliveCount = 0;
    let reachableCount = 0;
    let validCount = 0;

    for (let i = 0; i < totalNodesCount; i++) {
      const isAlive = nodeClient.isNodeAlive(i);
      const isPartitioned = nodeClient.isNodePartitioned(i);
      let isCorrupted = false;

      if (isAlive) {
        aliveCount++;
        if (!isPartitioned) {
          reachableCount++;
          if (targetMetadata && Array.isArray(targetMetadata.checksums) && targetMetadata.checksums[i]) {
            const shardObj = await nodeClient.readShard(i, targetMetadata.objectId, targetMetadata.checksums[i]);
            if (shardObj === null) {
              isCorrupted = true;
            }
          }
        }
      }

      if (isAlive && !isPartitioned && !isCorrupted) {
        validCount++;
      }

      let status = 'ONLINE';
      if (!isAlive) {
        status = 'DOWN';
      } else if (isPartitioned) {
        status = 'PARTITIONED';
      } else if (isCorrupted) {
        status = 'CORRUPTED';
      }

      nodes.push({
        nodeIndex: i,
        name: `node${i}`,
        role: i < k ? 'Data' : 'Parity',
        isAlive,
        isPartitioned,
        isReachable: isAlive && !isPartitioned,
        isCorrupted,
        status
      });
    }

    const partitionState = nodeClient.getPartitionState();

    let healthStatus = 'HEALTHY';
    let healthMessage = 'Healthy (Fault Tolerant: 1-2 failures/corruptions tolerable)';
    if (validCount === k) {
      healthStatus = 'DEGRADED';
      healthMessage = `Degraded (Minimum ${k} valid shards reachable — 1 more failure or partition breaks recovery)`;
    } else if (validCount < k) {
      healthStatus = 'IMPOSSIBLE';
      healthMessage = `Reconstruction impossible (${validCount}/${k} valid reachable shards available)`;
    }

    return res.status(200).json({
      totalNodes: totalNodesCount,
      aliveCount,
      reachableCount,
      validCount,
      deadCount: totalNodesCount - aliveCount,
      partitionState,
      healthStatus,
      healthMessage,
      activeObjectId: targetMetadata ? targetMetadata.objectId : null,
      nodes
    });
  } catch (err) {
    console.error('Error fetching node status:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Memory storage for multer to get Buffer directly
// Memory storage for multer to get Buffer directly
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 100 * 1024 * 1024 // 100MB limit
  }
});

/**
 * POST /upload
 * Multipart file upload.
 * Accepts optional durabilityPolicy { k, m } or k / m fields.
 * Accepts optional objectId to create a new version of an existing object.
 */
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ error: 'No file provided. Please upload a file with field name "file".' });
    }

    let existingMeta = null;
    let targetObjectId = req.body && req.body.objectId ? req.body.objectId.trim() : null;

    if (targetObjectId) {
      existingMeta = await nodeClient.getMetadata(targetObjectId);
    }

    // Determine version number
    let version = 1;
    if (existingMeta) {
      const highestVer = existingMeta.currentVersion || (Array.isArray(existingMeta.versions) ? existingMeta.versions.length : 1);
      version = highestVer + 1;
    }

    // Extract durability policy if specified (defaults to existing object's k/m or k=4, m=2)
    let k = existingMeta ? (existingMeta.k || 4) : 4;
    let m = existingMeta ? (existingMeta.m || 2) : 2;

    if (req.body) {
      if (req.body.durabilityPolicy) {
        let policy = req.body.durabilityPolicy;
        if (typeof policy === 'string') {
          try {
            policy = JSON.parse(policy);
          } catch (e) {
            // Keep default
          }
        }
        if (policy && typeof policy === 'object') {
          if (typeof policy.k === 'number' || typeof policy.k === 'string') {
            const parsedK = parseInt(policy.k, 10);
            if (!isNaN(parsedK) && parsedK >= 1 && parsedK <= 24) k = parsedK;
          }
          if (typeof policy.m === 'number' || typeof policy.m === 'string') {
            const parsedM = parseInt(policy.m, 10);
            if (!isNaN(parsedM) && parsedM >= 1 && parsedM <= 6) m = parsedM;
          }
        }
      }
      if (req.body.k !== undefined) {
        const parsedK = parseInt(req.body.k, 10);
        if (!isNaN(parsedK) && parsedK >= 1 && parsedK <= 24) k = parsedK;
      }
      if (req.body.m !== undefined) {
        const parsedM = parseInt(req.body.m, 10);
        if (!isNaN(parsedM) && parsedM >= 1 && parsedM <= 6) m = parsedM;
      }
    }

    const objectId = existingMeta ? targetObjectId : uuidv4();
    const originalName = req.file.originalname || `file-${objectId}`;
    const fileBuffer = req.file.buffer;
    const totalShards = k + m;

    // 1. Encode buffer into k data + m parity shards
    const { shards, shardSize, originalLength } = await encodeBuffer(fileBuffer, k, m);

    // 2. Write shards with version tag (e.g. <objectId>_v2.shard) to nodes 0..(k+m-1)
    const checksums = new Array(totalShards).fill(null);
    let successfulWrites = 0;

    for (const shard of shards) {
      if (nodeClient.isNodeAlive(shard.index)) {
        try {
          const checksum = await nodeClient.writeShard(shard.index, objectId, shard.data, version);
          checksums[shard.index] = checksum;
          successfulWrites++;
        } catch (err) {
          console.warn(`[WARN] Failed to write shard ${shard.index} v${version} to node${shard.index}: ${err.message}`);
        }
      }
    }

    if (successfulWrites < k) {
      return res.status(500).json({ error: `Upload failed: fewer than ${k} nodes available to store shards` });
    }

    // 3. Build version metadata record
    const versionRecord = {
      version,
      originalName,
      originalLength,
      shardSize,
      k,
      m,
      checksums,
      createdAt: new Date().toISOString()
    };

    let metadata;
    if (existingMeta) {
      metadata = existingMeta;
      if (!Array.isArray(metadata.versions)) {
        metadata.versions = [
          {
            version: 1,
            originalName: existingMeta.originalName,
            originalLength: existingMeta.originalLength,
            shardSize: existingMeta.shardSize,
            k: existingMeta.k || 4,
            m: existingMeta.m || 2,
            checksums: existingMeta.checksums || [],
            createdAt: existingMeta.createdAt
          }
        ];
      }
      metadata.versions.push(versionRecord);
      metadata.currentVersion = version;
      metadata.originalName = originalName;
      metadata.originalLength = originalLength;
      metadata.shardSize = shardSize;
      metadata.k = k;
      metadata.m = m;
      metadata.checksums = checksums;
      metadata.updatedAt = new Date().toISOString();
    } else {
      metadata = {
        objectId,
        originalName,
        originalLength,
        shardSize,
        k,
        m,
        currentVersion: 1,
        checksums,
        versions: [versionRecord],
        createdAt: new Date().toISOString()
      };
    }

    await nodeClient.saveMetadata(metadata);

    return res.status(201).json({ objectId, version, k, m });
  } catch (err) {
    console.error('Error during upload:', err);
    return res.status(500).json({ error: err.message || 'Internal server error during upload' });
  }
});

/**
 * GET /download/:objectId
 * Retrieves shards for the requested version (defaults to latest version).
 * Verifies version-tagged shard files and per-version checksums.
 */
app.get('/download/:objectId', async (req, res) => {
  try {
    const { objectId } = req.params;

    // 1. Read metadata
    const metadata = await nodeClient.getMetadata(objectId);
    if (!metadata) {
      return res.status(404).json({ error: `Object not found for objectId: ${objectId}` });
    }

    // Determine target version
    let targetVersion = req.query.version ? parseInt(req.query.version, 10) : null;
    if (targetVersion === null || isNaN(targetVersion)) {
      targetVersion = metadata.currentVersion || (Array.isArray(metadata.versions) ? metadata.versions[metadata.versions.length - 1].version : 1);
    }

    let targetVerMeta = null;
    if (Array.isArray(metadata.versions)) {
      targetVerMeta = metadata.versions.find((v) => v.version === targetVersion);
    }
    if (!targetVerMeta && targetVersion === 1) {
      targetVerMeta = metadata;
    }

    if (!targetVerMeta) {
      return res.status(404).json({ error: `Version ${targetVersion} not found for object: ${objectId}` });
    }

    const k = targetVerMeta.k || metadata.k || 4;
    const m = targetVerMeta.m || metadata.m || 2;
    const originalLength = targetVerMeta.originalLength;
    const originalName = targetVerMeta.originalName || metadata.originalName;
    const checksums = targetVerMeta.checksums || metadata.checksums || [];
    const totalShards = k + m;

    // 2. Query all required nodes for version-matching shards with checksum verification
    const readPromises = [];
    for (let i = 0; i < totalShards; i++) {
      const nodeIndex = (metadata.shardLocations && metadata.shardLocations[i] !== undefined)
        ? metadata.shardLocations[i]
        : ((targetVerMeta.shardLocations && targetVerMeta.shardLocations[i] !== undefined)
          ? targetVerMeta.shardLocations[i]
          : i);
      const expectedChecksum = (checksums && checksums[i]) ? checksums[i] : null;
      readPromises.push(
        nodeClient.readShard(nodeIndex, objectId, expectedChecksum, targetVersion).then((res) => {
          if (!res || !Buffer.isBuffer(res.data)) return null;
          return {
            index: i, // Logical shard index (0..k+m-1) required for Reed-Solomon decoding
            data: res.data
          };
        })
      );
    }
    const results = await Promise.all(readPromises);

    // 3. Filter out nulls (missing / offline / stale / corrupted shards)
    const availableShards = results.filter((s) => s !== null);

    // 4. Verify fault tolerance threshold (k valid shards required for this version)
    if (availableShards.length < k) {
      return res.status(500).json({
        error: 'Not enough shards to reconstruct — too many node failures',
        availableShardsCount: availableShards.length,
        requiredShardsCount: k,
        version: targetVersion
      });
    }

    // 5. Reconstruct original buffer using surviving valid shards with target version's parameters
    const reconstructedBuffer = await decodeShards(availableShards, originalLength, k, m);

    // 6. Set appropriate headers and send buffer
    const contentType = mime.lookup(originalName) || 'application/octet-stream';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(originalName)}"`);
    res.setHeader('Content-Length', reconstructedBuffer.length);
    res.setHeader('X-Object-Version', targetVersion);

    return res.send(reconstructedBuffer);
  } catch (err) {
    console.error('Error during download:', err);
    return res.status(500).json({ error: err.message || 'Internal server error during download' });
  }
});

/**
 * POST /simulate-stale-write/:nodeIndex/:objectId
 * Simulates a stale node by reverting it to an older version (e.g. v1 instead of v2).
 */
app.post('/simulate-stale-write/:nodeIndex/:objectId', async (req, res) => {
  try {
    const nodeIndex = parseInt(req.params.nodeIndex, 10);
    const { objectId } = req.params;
    const staleVersion = (req.body && req.body.staleVersion) ? parseInt(req.body.staleVersion, 10) : 1;

    if (isNaN(nodeIndex) || nodeIndex < 0) {
      return res.status(400).json({ error: 'nodeIndex must be a non-negative integer' });
    }

    const result = await nodeClient.simulateStaleWrite(nodeIndex, objectId, staleVersion);
    return res.status(200).json(result);
  } catch (err) {
    console.error('Error simulating stale write:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /objects
 * Lists all objects from the metadata folder.
 */
app.get('/objects', async (req, res) => {
  try {
    const objects = await nodeClient.listAllMetadata();
    return res.status(200).json(objects);
  } catch (err) {
    console.error('Error listing objects:', err);
    return res.status(500).json({ error: err.message || 'Failed to list objects' });
  }
});

/**
 * POST /corrupt-shard/:nodeIndex/:objectId and POST /corrupt-shard/:nodeIndex
 * Simulates corruption on a specific shard file by flipping bytes on disk.
 * Node remains online/reachable.
 */
async function handleCorruptShard(req, res) {
  try {
    const nodeIndex = parseInt(req.params.nodeIndex, 10);
    if (isNaN(nodeIndex) || nodeIndex < 0) {
      return res.status(400).json({ error: 'nodeIndex must be a non-negative integer' });
    }

    let objectId = req.params.objectId;
    if (!objectId) {
      const allObjects = await nodeClient.listAllMetadata();
      if (allObjects.length === 0) {
        return res.status(400).json({ error: 'No objects exist to corrupt. Please upload a file first.' });
      }
      objectId = allObjects[0].objectId;
    }

    const result = await nodeClient.corruptShard(nodeIndex, objectId);
    return res.status(200).json(result);
  } catch (err) {
    console.error('Error corrupting shard:', err);
    return res.status(500).json({ error: err.message });
  }
}

app.post('/corrupt-shard/:nodeIndex/:objectId', handleCorruptShard);
app.post('/corrupt-shard/:nodeIndex', handleCorruptShard);

/**
 * POST /simulate-failure/:nodeIndex
 * Renames nodes/node{N} to nodes/node{N}_DOWN to simulate a dead node.
 */
app.post('/simulate-failure/:nodeIndex', async (req, res) => {
  try {
    const nodeIndex = parseInt(req.params.nodeIndex, 10);
    if (isNaN(nodeIndex) || nodeIndex < 0) {
      return res.status(400).json({ error: 'nodeIndex must be a non-negative integer' });
    }

    await nodeClient.simulateFailure(nodeIndex);
    return res.status(200).json({
      success: true,
      message: `Node ${nodeIndex} simulated as failed (DOWN)`,
      nodeIndex,
      isAlive: nodeClient.isNodeAlive(nodeIndex)
    });
  } catch (err) {
    console.error('Error simulating failure:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /recover/:nodeIndex
 * Renames nodes/node{N}_DOWN back to nodes/node{N} to recover the node.
 */
app.post('/recover/:nodeIndex', async (req, res) => {
  try {
    const nodeIndex = parseInt(req.params.nodeIndex, 10);
    if (isNaN(nodeIndex) || nodeIndex < 0) {
      return res.status(400).json({ error: 'nodeIndex must be a non-negative integer' });
    }

    await nodeClient.recoverNode(nodeIndex);
    return res.status(200).json({
      success: true,
      message: `Node ${nodeIndex} recovered (UP)`,
      nodeIndex,
      isAlive: nodeClient.isNodeAlive(nodeIndex)
    });
  } catch (err) {
    console.error('Error recovering node:', err);
    return res.status(500).json({ error: err.message });
  }
});



/**
 * POST /nodes/add
 * Registers a new node joining the cluster (nodes/node{N}).
 */
app.post('/nodes/add', (req, res) => {
  try {
    const nextIndex = nodeClient.addNewNode();
    return res.status(201).json({
      success: true,
      nodeIndex: nextIndex,
      name: `node${nextIndex}`,
      message: `Node ${nextIndex} successfully joined the cluster.`
    });
  } catch (err) {
    console.error('Error adding node:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /nodes/load
 * Shows shard count and storage load distribution across all cluster nodes.
 */
app.get('/nodes/load', (req, res) => {
  try {
    const nodes = nodeClient.getAllNodesLoad();
    const totalShards = nodes.reduce((sum, n) => sum + n.shardCount, 0);
    return res.status(200).json({
      totalNodes: nodes.length,
      totalShards,
      nodes
    });
  } catch (err) {
    console.error('Error getting nodes load:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /rebalance
 * Manually triggers a rebalance scan to redistribute shards across nodes.
 */
app.post('/rebalance', async (req, res) => {
  try {
    const result = await rebalance.runRebalanceScan();
    return res.status(200).json({
      success: true,
      ...result
    });
  } catch (err) {
    console.error('Error during rebalance:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /repair/status
 * Returns last repair scan timestamp and recent repairs for dashboard visibility.
 */
app.get('/repair/status', (req, res) => {
  try {
    const status = repair.getLastRepairStatus();
    return res.status(200).json(status);
  } catch (err) {
    console.error('Error getting repair status:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /repair/scan
 * Manually triggers a background self-healing repair scan across all objects.
 */
app.post('/repair/scan', async (req, res) => {
  try {
    const result = await repair.runRepairScan();
    return res.status(200).json({
      success: true,
      ...result
    });
  } catch (err) {
    console.error('Error running manual repair scan:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /simulate-partition
 * Simulates a network partition splitting nodes into two groups (e.g. { groupA: [0,1,2], groupB: [3,4,5], activeGroup: 'groupA' }).
 */
app.post('/simulate-partition', (req, res) => {
  try {
    const { groupA, groupB, activeGroup = 'groupA' } = req.body || {};
    if (!Array.isArray(groupA) || !Array.isArray(groupB)) {
      return res.status(400).json({ error: 'Body must include groupA and groupB as arrays of node indices' });
    }

    const state = nodeClient.setNetworkPartition({ groupA, groupB, activeGroup });
    return res.status(200).json({
      success: true,
      message: `Network partition simulated. Reachable group (${activeGroup}): [${state.reachableNodes.join(', ')}], Unreachable: [${state.unreachableNodes.join(', ')}]`,
      ...state
    });
  } catch (err) {
    console.error('Error simulating partition:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /heal-partition
 * Clears network partition and restores full cluster connectivity.
 */
app.post('/heal-partition', (req, res) => {
  try {
    const state = nodeClient.healNetworkPartition();
    return res.status(200).json({
      success: true,
      message: 'Network partition healed. All online nodes are fully reachable.',
      ...state
    });
  } catch (err) {
    console.error('Error healing partition:', err);
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /partition/status
 * Returns current partition state.
 */
app.get('/partition/status', (req, res) => {
  try {
    const state = nodeClient.getPartitionState();
    return res.status(200).json(state);
  } catch (err) {
    console.error('Error getting partition status:', err);
    return res.status(500).json({ error: err.message });
  }
});

module.exports = app;
