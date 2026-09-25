'use strict';

const fs = require('fs');
const path = require('path');
const nodeClient = require('./nodeClient.js');

/**
 * Runs a rebalance scan across all objects in the cluster.
 * Identifies uneven shard distribution across storage nodes and redistributes
 * shards from overloaded nodes to underloaded nodes.
 *
 * @param {number} [maxMoves=500] - Maximum shard migrations per scan
 * @returns {Promise<{rebalanced: boolean, movesCount: number, moves: Array<{objectId: string, shardIndex: number, fromNode: number, toNode: number, version: number}>, finalLoad: Array}>}
 */
async function runRebalanceScan(maxMoves = 500) {
  const moves = [];
  const movedKeys = new Set();

  for (let step = 0; step < maxMoves; step++) {
    const nodesLoad = nodeClient.getAllNodesLoad().filter((n) => n.isAlive);
    if (nodesLoad.length <= 1) break;

    // Sort nodes ascending by shard count
    nodesLoad.sort((a, b) => a.shardCount - b.shardCount);

    const minNode = nodesLoad[0];
    const maxNode = nodesLoad[nodesLoad.length - 1];

    // Check if cluster is sufficiently balanced (difference <= 1)
    if (maxNode.shardCount - minNode.shardCount <= 1) {
      break;
    }

    // Find a candidate shard on maxNode that can be moved to minNode
    const allObjects = await nodeClient.listAllMetadata();
    let movedInThisIteration = false;

    for (const item of allObjects) {
      const meta = await nodeClient.getMetadata(item.objectId);
      if (!meta) continue;

      const totalShards = (meta.k || 4) + (meta.m || 2);
      const currentVer = meta.currentVersion || 1;
      const locations = Array.isArray(meta.shardLocations)
        ? [...meta.shardLocations]
        : Array.from({ length: totalShards }, (_, i) => i);

      // Check if this object already has ANY shard on minNode
      const minNodeAlreadyHasShard = locations.includes(minNode.nodeIndex);
      if (minNodeAlreadyHasShard) {
        continue; // Keep fault domain isolated: at most 1 shard per node for an object
      }

      // Find which shard of this object is located on maxNode
      const shardIndexOnMaxNode = locations.findIndex((loc) => loc === maxNode.nodeIndex);
      if (shardIndexOnMaxNode === -1) {
        continue;
      }

      const moveKey = `${meta.objectId}:${shardIndexOnMaxNode}`;
      if (movedKeys.has(moveKey)) {
        continue;
      }

      // Shard found on maxNode! Proceed with atomic migration
      const expectedChecksum = (meta.checksums && meta.checksums[shardIndexOnMaxNode]) || null;
      const shardObj = await nodeClient.readShard(maxNode.nodeIndex, meta.objectId, expectedChecksum, currentVer);

      if (!shardObj || !Buffer.isBuffer(shardObj.data)) {
        continue; // Skip invalid/unreadable shard
      }

      // 1. Write shard to underloaded destination node
      const writtenChecksum = await nodeClient.writeShard(minNode.nodeIndex, meta.objectId, shardObj.data, currentVer);

      // 2. Verify checksum on destination node
      const verifyObj = await nodeClient.readShard(minNode.nodeIndex, meta.objectId, writtenChecksum, currentVer);
      if (!verifyObj) {
        // Rollback write if verification failed
        await nodeClient.deleteShard(minNode.nodeIndex, meta.objectId, currentVer);
        continue;
      }

      // 3. Delete shard from overloaded source node
      await nodeClient.deleteShard(maxNode.nodeIndex, meta.objectId, currentVer);

      // 4. Update metadata shard locations
      locations[shardIndexOnMaxNode] = minNode.nodeIndex;
      meta.shardLocations = locations;
      await nodeClient.saveMetadata(meta);

      movedKeys.add(moveKey);

      moves.push({
        objectId: meta.objectId,
        shardIndex: shardIndexOnMaxNode,
        fromNode: maxNode.nodeIndex,
        toNode: minNode.nodeIndex,
        version: currentVer
      });

      console.log(`[REBALANCE] Migrated shard ${shardIndexOnMaxNode} of object ${meta.objectId} from node${maxNode.nodeIndex} -> node${minNode.nodeIndex}`);
      movedInThisIteration = true;
      break; // Re-evaluate cluster load after each migration
    }

    if (!movedInThisIteration) {
      // No more candidate migrations could be safely made
      break;
    }
  }

  return {
    rebalanced: moves.length > 0,
    movesCount: moves.length,
    moves,
    finalLoad: nodeClient.getAllNodesLoad()
  };
}

module.exports = {
  runRebalanceScan
};
