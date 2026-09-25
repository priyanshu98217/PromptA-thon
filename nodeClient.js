'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const BASE_DIR = __dirname;
const NODES_DIR = path.join(BASE_DIR, 'nodes');
const METADATA_DIR = path.join(BASE_DIR, 'metadata');
const TOTAL_NODES = 6;

/**
 * Computes SHA-256 hash of a buffer.
 *
 * @param {Buffer} buffer
 * @returns {string} Hexadecimal SHA-256 checksum
 */
function computeChecksum(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

/**
 * Ensures that a node directory exists (or remains down if it was marked as _DOWN).
 * Dynamically creates nodes/node{N} on demand.
 *
 * @param {number} nodeIndex
 */
function ensureNode(nodeIndex) {
  if (!fs.existsSync(NODES_DIR)) {
    fs.mkdirSync(NODES_DIR, { recursive: true });
  }
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  const downPath = path.join(NODES_DIR, `node${nodeIndex}_DOWN`);
  if (!fs.existsSync(nodePath) && !fs.existsSync(downPath)) {
    fs.mkdirSync(nodePath, { recursive: true });
  }
}

/**
 * Initializes nodes/ and metadata/ directory structures for default nodes 0..5.
 */
function initStorage() {
  if (!fs.existsSync(NODES_DIR)) {
    fs.mkdirSync(NODES_DIR, { recursive: true });
  }
  if (!fs.existsSync(METADATA_DIR)) {
    fs.mkdirSync(METADATA_DIR, { recursive: true });
  }

  for (let i = 0; i < TOTAL_NODES; i++) {
    ensureNode(i);
  }
}

/**
 * Lists all existing node indices in the cluster (both online and down).
 *
 * @returns {number[]} Sorted array of integer node indices
 */
function listAllNodeIndices() {
  const indices = new Set([0, 1, 2, 3, 4, 5]);
  if (fs.existsSync(NODES_DIR)) {
    const entries = fs.readdirSync(NODES_DIR);
    for (const entry of entries) {
      const match = entry.match(/^node(\d+)(?:_DOWN)?$/);
      if (match) {
        indices.add(parseInt(match[1], 10));
      }
    }
  }
  return Array.from(indices).sort((a, b) => a - b);
}

/**
 * Registers a brand new node joining the cluster by creating nodes/node{N}.
 *
 * @returns {number} The new node's index
 */
function addNewNode() {
  const allIndices = listAllNodeIndices();
  const nextIndex = Math.max(...allIndices) + 1;
  const nodePath = path.join(NODES_DIR, `node${nextIndex}`);
  if (!fs.existsSync(nodePath)) {
    fs.mkdirSync(nodePath, { recursive: true });
  }
  return nextIndex;
}

/**
 * Retrieves the current shard count and status of all storage nodes.
 *
 * @returns {Array<{nodeIndex: number, name: string, isAlive: boolean, shardCount: number, shardFiles: string[]}>}
 */
function getAllNodesLoad() {
  const allIndices = listAllNodeIndices();
  const results = [];

  for (const idx of allIndices) {
    const isAlive = isNodeAlive(idx);
    const nodePath = path.join(NODES_DIR, `node${idx}`);
    let shardFiles = [];

    if (isAlive && fs.existsSync(nodePath)) {
      try {
        shardFiles = fs.readdirSync(nodePath).filter((f) => f.endsWith('.shard'));
      } catch (e) {}
    }

    results.push({
      nodeIndex: idx,
      name: `node${idx}`,
      isAlive,
      shardCount: shardFiles.length,
      shardFiles
    });
  }

  return results;
}

async function safeUnlink(filePath, retries = 5, delayMs = 60) {
  if (!fs.existsSync(filePath)) return;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await fs.promises.unlink(filePath);
      return;
    } catch (err) {
      if ((err.code === 'EPERM' || err.code === 'EBUSY' || err.code === 'EACCES') && attempt < retries - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      } else if (err.code === 'ENOENT') {
        return;
      } else {
        throw err;
      }
    }
  }
}

/**
 * Deletes a shard file for a given object on a specific node.
 *
 * @param {number} nodeIndex
 * @param {string} objectId
 * @param {number} [version=null]
 */
async function deleteShard(nodeIndex, objectId, version = null) {
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  if (!fs.existsSync(nodePath)) return;

  if (version !== null && version !== undefined) {
    const vPath = path.join(nodePath, `${objectId}_v${version}.shard`);
    await safeUnlink(vPath);
    if (version === 1) {
      const legacyPath = path.join(nodePath, `${objectId}.shard`);
      await safeUnlink(legacyPath);
    }
  } else {
    const files = await fs.promises.readdir(nodePath);
    for (const file of files) {
      if (file.startsWith(objectId) && file.endsWith('.shard')) {
        await safeUnlink(path.join(nodePath, file));
      }
    }
  }
}

let partitionState = {
  active: false,
  groupA: [],
  groupB: [],
  activeGroup: 'groupA',
  reachableNodes: new Set(),
  partitionedNodes: new Set()
};

/**
 * Checks if a node is currently unreachable due to an active network partition.
 *
 * @param {number} nodeIndex
 * @returns {boolean}
 */
function isNodePartitioned(nodeIndex) {
  if (!partitionState.active) return false;
  return partitionState.partitionedNodes.has(nodeIndex);
}

/**
 * Simulates a network partition splitting cluster into two disconnected groups.
 *
 * @param {object} params
 * @param {number[]} params.groupA
 * @param {number[]} params.groupB
 * @param {string} [params.activeGroup='groupA'] - The group reachable from client/API layer
 * @returns {object} Current partition state
 */
function setNetworkPartition({ groupA = [], groupB = [], activeGroup = 'groupA' } = {}) {
  const normA = Array.isArray(groupA) ? groupA.map((n) => parseInt(n, 10)).filter((n) => !isNaN(n)) : [];
  const normB = Array.isArray(groupB) ? groupB.map((n) => parseInt(n, 10)).filter((n) => !isNaN(n)) : [];

  const reachable = activeGroup === 'groupB' ? normB : normA;
  const unreachable = activeGroup === 'groupB' ? normA : normB;

  partitionState = {
    active: true,
    groupA: normA,
    groupB: normB,
    activeGroup,
    reachableNodes: new Set(reachable),
    partitionedNodes: new Set(unreachable)
  };

  console.log(`[PARTITION] Network partition activated. Active reachable group (${activeGroup}): [${reachable.join(', ')}]. Partitioned unreachable: [${unreachable.join(', ')}]`);
  return getPartitionState();
}

/**
 * Heals the active network partition and restores full cluster reachability.
 *
 * @returns {object} Current partition state
 */
function healNetworkPartition() {
  partitionState = {
    active: false,
    groupA: [],
    groupB: [],
    activeGroup: 'groupA',
    reachableNodes: new Set(),
    partitionedNodes: new Set()
  };
  console.log('[PARTITION] Network partition healed. All online nodes are fully reachable.');
  return getPartitionState();
}

/**
 * Returns current network partition state.
 *
 * @returns {object}
 */
function getPartitionState() {
  return {
    active: partitionState.active,
    groupA: partitionState.groupA,
    groupB: partitionState.groupB,
    activeGroup: partitionState.activeGroup,
    reachableNodes: Array.from(partitionState.reachableNodes),
    unreachableNodes: Array.from(partitionState.partitionedNodes)
  };
}

/**
 * Checks if a specific node is alive (node folder exists and is not renamed to _DOWN).
 * Automatically initializes node directory on demand if not previously created.
 *
 * @param {number} nodeIndex - Node index
 * @returns {boolean}
 */
function isNodeAlive(nodeIndex) {
  ensureNode(nodeIndex);
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  return fs.existsSync(nodePath) && fs.statSync(nodePath).isDirectory();
}

/**
 * Writes a shard buffer to a specific node's folder and computes its SHA-256 checksum.
 * Saves shard with version-tagged filename (e.g. <objectId>_v2.shard).
 *
 * @param {number} nodeIndex - Node index
 * @param {string} objectId - Unique object ID
 * @param {Buffer} buffer - Shard data buffer
 * @param {number} [version=1] - Version number
 * @returns {Promise<string>} The computed SHA-256 checksum
 */
async function writeShard(nodeIndex, objectId, buffer, version = 1) {
  ensureNode(nodeIndex);
  if (isNodePartitioned(nodeIndex)) {
    console.warn(`[PARTITION] node${nodeIndex} unreachable due to active network partition`);
    throw new Error(`Node ${nodeIndex} is unreachable due to active network partition`);
  }
  if (!isNodeAlive(nodeIndex)) {
    console.warn(`[DOWN] node${nodeIndex} offline`);
    throw new Error(`Cannot write shard: Node ${nodeIndex} is offline or does not exist`);
  }

  const checksum = computeChecksum(buffer);
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  const vShardFilePath = path.join(nodePath, `${objectId}_v${version}.shard`);
  await fs.promises.writeFile(vShardFilePath, buffer);

  // For version 1, also write legacy filename for seamless backward compatibility
  if (version === 1) {
    const legacyShardFilePath = path.join(nodePath, `${objectId}.shard`);
    await fs.promises.writeFile(legacyShardFilePath, buffer);
  }

  return checksum;
}

/**
 * Persists a shard's SHA-256 checksum into the object's metadata.
 *
 * @param {number} nodeIndex - Node index (0-5)
 * @param {string} objectId - Unique object ID
 * @param {string} checksum - SHA-256 checksum
 */
async function saveShardChecksum(nodeIndex, objectId, checksum) {
  const metadata = await getMetadata(objectId);
  if (!metadata) {
    throw new Error(`Cannot save checksum: Metadata not found for object ${objectId}`);
  }
  if (!Array.isArray(metadata.checksums)) {
    metadata.checksums = new Array(TOTAL_NODES).fill(null);
  }
  metadata.checksums[nodeIndex] = checksum;
  await saveMetadata(metadata);
}

/**
 * Reads a shard from a specific node's folder with version and checksum verification.
 * Returns { index, data } if valid, or null if missing, dead, stale, or corrupted.
 *
 * @param {number} nodeIndex - Node index
 * @param {string} objectId - Unique object ID
 * @param {string} [expectedChecksum] - Expected SHA-256 checksum
 * @param {number} [expectedVersion] - Expected version number
 * @returns {Promise<{index: number, data: Buffer}|null>}
 */
async function readShard(nodeIndex, objectId, expectedChecksum, expectedVersion = null) {
  try {
    if (isNodePartitioned(nodeIndex)) {
      console.warn(`[PARTITION] node${nodeIndex} unreachable due to active network partition`);
      return null;
    }
    if (!isNodeAlive(nodeIndex)) {
      console.warn(`[DOWN] node${nodeIndex} offline`);
      return null;
    }

    const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
    let shardFilePath = null;

    if (expectedVersion !== null && expectedVersion !== undefined) {
      const vPath = path.join(nodePath, `${objectId}_v${expectedVersion}.shard`);
      if (fs.existsSync(vPath)) {
        shardFilePath = vPath;
      } else if (expectedVersion === 1) {
        const legacyPath = path.join(nodePath, `${objectId}.shard`);
        if (fs.existsSync(legacyPath)) {
          shardFilePath = legacyPath;
        }
      }
    } else {
      const legacyPath = path.join(nodePath, `${objectId}.shard`);
      if (fs.existsSync(legacyPath)) {
        shardFilePath = legacyPath;
      } else if (fs.existsSync(nodePath)) {
        const files = await fs.promises.readdir(nodePath);
        const vFiles = files.filter(f => f.startsWith(`${objectId}_v`) && f.endsWith('.shard'));
        if (vFiles.length > 0) {
          vFiles.sort();
          shardFilePath = path.join(nodePath, vFiles[vFiles.length - 1]);
        }
      }
    }

    if (!shardFilePath || !fs.existsSync(shardFilePath)) {
      return null; // Missing or stale shard
    }

    const data = await fs.promises.readFile(shardFilePath);

    // Checksum integrity verification
    if (expectedChecksum) {
      const actualChecksum = computeChecksum(data);
      if (actualChecksum !== expectedChecksum) {
        console.warn(`[WARN] Shard checksum mismatch on node${nodeIndex} for object ${objectId} (expected: ${expectedChecksum.slice(0, 8)}..., got: ${actualChecksum.slice(0, 8)}...)`);
        return null; // Treat corrupted or stale shard as unavailable
      }
    }

    return {
      index: nodeIndex,
      data
    };
  } catch (err) {
    return null;
  }
}

/**
 * Simulates corruption on a specific live shard file by flipping bytes on disk.
 *
 * @param {number} nodeIndex - Node index
 * @param {string} objectId - Unique object ID
 * @param {number} [version=null] - Specific version to corrupt
 * @returns {Promise<{success: boolean, message: string, nodeIndex: number, objectId: string}>}
 */
async function corruptShard(nodeIndex, objectId, version = null) {
  if (!isNodeAlive(nodeIndex)) {
    throw new Error(`Cannot corrupt shard: Node ${nodeIndex} is offline or does not exist`);
  }

  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  let shardFilePath = null;

  if (version !== null && version !== undefined) {
    const vPath = path.join(nodePath, `${objectId}_v${version}.shard`);
    if (fs.existsSync(vPath)) {
      shardFilePath = vPath;
    } else if (version === 1) {
      const legacyPath = path.join(nodePath, `${objectId}.shard`);
      if (fs.existsSync(legacyPath)) shardFilePath = legacyPath;
    }
  } else {
    const legacyPath = path.join(nodePath, `${objectId}.shard`);
    if (fs.existsSync(legacyPath)) {
      shardFilePath = legacyPath;
    } else if (fs.existsSync(nodePath)) {
      const files = await fs.promises.readdir(nodePath);
      const vFiles = files.filter(f => f.startsWith(`${objectId}_v`) && f.endsWith('.shard'));
      if (vFiles.length > 0) {
        vFiles.sort();
        shardFilePath = path.join(nodePath, vFiles[vFiles.length - 1]);
      }
    }
  }

  if (!shardFilePath || !fs.existsSync(shardFilePath)) {
    throw new Error(`Shard file not found for object ${objectId} on node ${nodeIndex}`);
  }

  const data = await fs.promises.readFile(shardFilePath);
  // Flip bits in the shard
  for (let i = 0; i < Math.min(8, data.length); i++) {
    data[i] ^= 0xFF;
  }
  if (data.length > 8) {
    data[data.length - 1] ^= 0xAA;
  }

  await fs.promises.writeFile(shardFilePath, data);

  if (shardFilePath.endsWith('_v1.shard')) {
    const legacy = path.join(nodePath, `${objectId}.shard`);
    if (fs.existsSync(legacy)) await fs.promises.writeFile(legacy, data);
  } else if (shardFilePath.endsWith(`${objectId}.shard`)) {
    const v1 = path.join(nodePath, `${objectId}_v1.shard`);
    if (fs.existsSync(v1)) await fs.promises.writeFile(v1, data);
  }

  return {
    success: true,
    message: `Shard on node ${nodeIndex} corrupted for object ${objectId}`,
    nodeIndex,
    objectId
  };
}

/**
 * Simulates a stale write by removing newer version shards on a node,
 * leaving it with an older version's shard (e.g. v1 instead of v2).
 *
 * @param {number} nodeIndex - Node index
 * @param {string} objectId - Unique object ID
 * @param {number} [targetStaleVersion=1] - Version the node remains on
 * @returns {Promise<{success: boolean, message: string, nodeIndex: number, objectId: string, staleVersion: number, currentClusterVersion: number}>}
 */
async function simulateStaleWrite(nodeIndex, objectId, targetStaleVersion = 1) {
  if (!isNodeAlive(nodeIndex)) {
    throw new Error(`Cannot simulate stale write: Node ${nodeIndex} is offline or does not exist`);
  }

  const metadata = await getMetadata(objectId);
  if (!metadata) {
    throw new Error(`Object not found for objectId: ${objectId}`);
  }

  const currentVer = metadata.currentVersion || 1;
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);

  // Delete all versions above targetStaleVersion on this node
  for (let v = targetStaleVersion + 1; v <= currentVer; v++) {
    const vPath = path.join(nodePath, `${objectId}_v${v}.shard`);
    await safeUnlink(vPath);
  }

  return {
    success: true,
    message: `Node ${nodeIndex} simulated with stale write (reverted to v${targetStaleVersion} while cluster current is v${currentVer})`,
    nodeIndex,
    objectId,
    staleVersion: targetStaleVersion,
    currentClusterVersion: currentVer
  };
}

async function safeRename(src, dest, retries = 5, delayMs = 60) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await fs.promises.rename(src, dest);
      return;
    } catch (err) {
      if ((err.code === 'EPERM' || err.code === 'EBUSY' || err.code === 'EACCES') && attempt < retries - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      } else {
        throw err;
      }
    }
  }
}

/**
 * Renames nodes/node{N} to nodes/node{N}_DOWN to simulate a dead node.
 *
 * @param {number} nodeIndex - Node index
 */
async function simulateFailure(nodeIndex) {
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  const downPath = path.join(NODES_DIR, `node${nodeIndex}_DOWN`);

  if (fs.existsSync(downPath)) {
    // Already down
    return;
  }

  if (fs.existsSync(nodePath)) {
    await safeRename(nodePath, downPath);
  } else {
    // If neither exists, create as down
    await fs.promises.mkdir(downPath, { recursive: true });
  }
}

/**
 * Renames nodes/node{N}_DOWN back to nodes/node{N} to recover a dead node.
 *
 * @param {number} nodeIndex - Node index
 */
async function recoverNode(nodeIndex) {
  const nodePath = path.join(NODES_DIR, `node${nodeIndex}`);
  const downPath = path.join(NODES_DIR, `node${nodeIndex}_DOWN`);

  if (fs.existsSync(downPath)) {
    await safeRename(downPath, nodePath);
  } else if (!fs.existsSync(nodePath)) {
    await fs.promises.mkdir(nodePath, { recursive: true });
  }
}

/**
 * Saves metadata JSON for an object to metadata/<objectId>.json.
 *
 * @param {object} metadata
 */
async function saveMetadata(metadata) {
  if (!fs.existsSync(METADATA_DIR)) {
    await fs.promises.mkdir(METADATA_DIR, { recursive: true });
  }
  const metaPath = path.join(METADATA_DIR, `${metadata.objectId}.json`);
  await fs.promises.writeFile(metaPath, JSON.stringify(metadata, null, 2), 'utf8');
}

/**
 * Retrieves metadata JSON for an object.
 *
 * @param {string} objectId
 * @returns {Promise<object|null>}
 */
async function getMetadata(objectId) {
  try {
    const metaPath = path.join(METADATA_DIR, `${objectId}.json`);
    if (!fs.existsSync(metaPath)) {
      return null;
    }
    const content = await fs.promises.readFile(metaPath, 'utf8');
    return JSON.parse(content);
  } catch (err) {
    return null;
  }
}

/**
 * Lists all objects stored in metadata directory.
 *
 * @returns {Promise<Array<{objectId: string, originalName: string, checksums: string[], createdAt: string}>>}
 */
async function listAllMetadata() {
  if (!fs.existsSync(METADATA_DIR)) {
    return [];
  }
  const files = await fs.promises.readdir(METADATA_DIR);
  const jsonFiles = files.filter((f) => f.endsWith('.json'));

  const results = [];
  for (const file of jsonFiles) {
    try {
      const content = await fs.promises.readFile(path.join(METADATA_DIR, file), 'utf8');
      const meta = JSON.parse(content);
      results.push({
        objectId: meta.objectId,
        originalName: meta.originalName,
        checksums: meta.checksums || [],
        createdAt: meta.createdAt
      });
    } catch (e) {
      // Ignore corrupted metadata files
    }
  }

  // Sort descending by createdAt
  results.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  return results;
}

// Automatically ensure folder structure on import
initStorage();

module.exports = {
  TOTAL_NODES,
  NODES_DIR,
  METADATA_DIR,
  computeChecksum,
  initStorage,
  isNodeAlive,
  writeShard,
  saveShardChecksum,
  readShard,
  corruptShard,
  simulateStaleWrite,
  simulateFailure,
  recoverNode,
  listAllNodeIndices,
  addNewNode,
  getAllNodesLoad,
  deleteShard,
  isNodePartitioned,
  setNetworkPartition,
  healNetworkPartition,
  getPartitionState,
  saveMetadata,
  getMetadata,
  listAllMetadata
};
