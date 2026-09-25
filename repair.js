'use strict';

const fs = require('fs');
const path = require('path');
const nodeClient = require('./nodeClient.js');
const { encodeBuffer, decodeShards } = require('./erasure.js');

let lastScanTime = null;
let lastRepairs = [];
let isScanning = false;
let repairTimer = null;

/**
 * Executes a full repair scan across all stored objects in the cluster.
 * Detects missing or corrupted shards on alive storage nodes and restores them
 * using Reed-Solomon reconstruction.
 *
 * @returns {Promise<{lastScanTime: string, repairs: Array<{objectId: string, nodeIndex: number, reason: string, timestamp: string}>}>}
 */
async function runRepairScan() {
  if (isScanning) {
    return {
      lastScanTime,
      repairs: lastRepairs,
      isScanning: true
    };
  }

  isScanning = true;
  const currentRepairs = [];

  try {
    const objectList = await nodeClient.listAllMetadata();

    for (const item of objectList) {
      const meta = await nodeClient.getMetadata(item.objectId);
      if (!meta) continue;

      const currentVersion = meta.currentVersion || (Array.isArray(meta.versions) ? meta.versions[meta.versions.length - 1].version : 1);
      const k = meta.k || 4;
      const m = meta.m || 2;
      const totalShards = k + m;
      const expectedChecksums = Array.isArray(meta.checksums) ? meta.checksums : [];

      const validShards = [];
      const nodesNeedingRepair = [];

      for (let i = 0; i < totalShards; i++) {
        const targetNode = (meta.shardLocations && meta.shardLocations[i] !== undefined) ? meta.shardLocations[i] : i;
        if (!nodeClient.isNodeAlive(targetNode)) {
          // Node is offline/down - cannot repair an unreachable node
          continue;
        }

        const expectedChecksum = expectedChecksums[i] || null;
        const shardObj = await nodeClient.readShard(targetNode, meta.objectId, expectedChecksum, currentVersion);

        if (shardObj !== null && Buffer.isBuffer(shardObj.data)) {
          validShards.push({ ...shardObj, index: i });
        } else {
          // Shard is missing, stale, or corrupted on an alive node
          const vShardPath = path.join(nodeClient.NODES_DIR, `node${targetNode}`, `${meta.objectId}_v${currentVersion}.shard`);
          const legacyShardPath = path.join(nodeClient.NODES_DIR, `node${targetNode}`, `${meta.objectId}.shard`);
          const reason = (fs.existsSync(vShardPath) || fs.existsSync(legacyShardPath)) ? 'corrupted' : 'missing';
          nodesNeedingRepair.push({ shardIndex: i, nodeIndex: targetNode, reason });
        }
      }

      // Self-heal only if we have sufficient quorum (>= k valid shards) and nodes needing repair
      if (nodesNeedingRepair.length > 0 && validShards.length >= k) {
        try {
          // 1. Reconstruct original data buffer from surviving valid shards
          const reconstructedBuffer = await decodeShards(validShards, meta.originalLength, k, m);

          // 2. Re-encode full set of k+m shards
          const { shards: encodedShards } = await encodeBuffer(reconstructedBuffer, k, m);

          // 3. Write recovered shards back to affected nodes
          let metadataUpdated = false;
          for (const repairTarget of nodesNeedingRepair) {
            const nodeIdx = repairTarget.nodeIndex;
            const shardIdx = repairTarget.shardIndex;
            if (nodeClient.isNodeAlive(nodeIdx)) {
              const shardToRestore = encodedShards.find((s) => s.index === shardIdx);
              if (shardToRestore) {
                const newChecksum = await nodeClient.writeShard(nodeIdx, meta.objectId, shardToRestore.data, currentVersion);
                if (!Array.isArray(meta.checksums)) {
                  meta.checksums = new Array(totalShards).fill(null);
                }
                meta.checksums[shardIdx] = newChecksum;
                metadataUpdated = true;

                const repairRecord = {
                  objectId: meta.objectId,
                  nodeIndex: nodeIdx,
                  shardIndex: shardIdx,
                  version: currentVersion,
                  reason: repairTarget.reason,
                  timestamp: new Date().toISOString()
                };
                currentRepairs.push(repairRecord);
                console.log(`[REPAIR] Restored shard ${shardIdx} on node${nodeIdx}, object ${meta.objectId} v${currentVersion} (${repairTarget.reason})`);
              }
            }
          }

          if (metadataUpdated) {
            // Also update version entry in versions array if present
            if (Array.isArray(meta.versions)) {
              const verObj = meta.versions.find(v => v.version === currentVersion);
              if (verObj) {
                verObj.checksums = meta.checksums;
              }
            }
            await nodeClient.saveMetadata(meta);
          }
        } catch (repairErr) {
          console.error(`[REPAIR ERROR] Failed to heal object ${meta.objectId}:`, repairErr.message);
        }
      }
    }

    lastScanTime = new Date().toISOString();
    if (currentRepairs.length > 0) {
      lastRepairs = [...currentRepairs, ...lastRepairs].slice(0, 50); // Keep last 50 repairs
    }
  } catch (err) {
    console.error('[REPAIR SCAN ERROR]:', err);
  } finally {
    isScanning = false;
  }

  return {
    lastScanTime,
    repairs: currentRepairs
  };
}

/**
 * Returns current status of the background repair service.
 */
function getLastRepairStatus() {
  return {
    lastScanTime,
    repairsCount: lastRepairs.length,
    recentRepairs: lastRepairs,
    isScanning
  };
}

/**
 * Starts automatic recurring background repair scan.
 *
 * @param {number} intervalMs - Default: 15,000 ms (15s)
 */
function startBackgroundRepair(intervalMs = 15000) {
  if (repairTimer) {
    clearInterval(repairTimer);
  }
  repairTimer = setInterval(async () => {
    try {
      await runRepairScan();
    } catch (err) {
      console.error('[BACKGROUND REPAIR TIMER ERROR]:', err);
    }
  }, intervalMs);

  if (typeof repairTimer.unref === 'function') {
    repairTimer.unref(); // Avoid holding Node process open if exiting
  }
}

/**
 * Stops recurring background repair scan.
 */
function stopBackgroundRepair() {
  if (repairTimer) {
    clearInterval(repairTimer);
    repairTimer = null;
  }
}

module.exports = {
  runRepairScan,
  getLastRepairStatus,
  startBackgroundRepair,
  stopBackgroundRepair
};
