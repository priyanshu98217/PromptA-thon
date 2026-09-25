'use strict';

const ReedSolomon = require('@ronomon/reed-solomon');

const K = 4; // 4 data shards
const M = 2; // 2 parity shards

/**
 * Calculates the required shard size for a given buffer length.
 * Shard size must be a multiple of 8 bytes for @ronomon/reed-solomon.
 *
 * @param {number} bufferLength
 * @returns {number}
 */
function calculateShardSize(bufferLength, k = K) {
  const minBytesPerShard = Math.ceil(bufferLength / k);
  return Math.max(8, Math.ceil(minBytesPerShard / 8) * 8);
}

/**
 * Encodes a buffer into k data shards + m parity shards (defaults to k=4, m=2).
 *
 * @param {Buffer|Uint8Array|string} buffer - The input data buffer to encode.
 * @param {number|{k?: number, m?: number}} [kOrOptions=4] - Number of data shards or options object.
 * @param {number} [m=2] - Number of parity shards.
 * @returns {Promise<{shards: Array<{index: number, data: Buffer}>, shardSize: number, originalLength: number, k: number, m: number}>}
 */
async function encodeBuffer(buffer, kOrOptions = K, m = M) {
  let k = K;
  let mVal = M;

  if (typeof kOrOptions === 'object' && kOrOptions !== null) {
    k = typeof kOrOptions.k === 'number' ? kOrOptions.k : K;
    mVal = typeof kOrOptions.m === 'number' ? kOrOptions.m : M;
  } else if (typeof kOrOptions === 'number') {
    k = kOrOptions;
    if (typeof m === 'number') {
      mVal = m;
    }
  }

  if (!Buffer.isBuffer(buffer)) {
    buffer = Buffer.from(buffer);
  }

  const originalLength = buffer.length;
  const shardSize = calculateShardSize(originalLength, k);
  const bufferSize = shardSize * k;
  const paritySize = shardSize * mVal;

  const dataBuffer = Buffer.alloc(bufferSize);
  buffer.copy(dataBuffer, 0);

  const parityBuffer = Buffer.alloc(paritySize);

  const context = ReedSolomon.create(k, mVal);

  let sources = 0;
  for (let i = 0; i < k; i++) {
    sources |= (1 << i);
  }

  let targets = 0;
  for (let i = k; i < k + mVal; i++) {
    targets |= (1 << i);
  }

  await new Promise((resolve, reject) => {
    ReedSolomon.encode(
      context,
      sources,
      targets,
      dataBuffer,
      0,
      bufferSize,
      parityBuffer,
      0,
      paritySize,
      (err) => {
        if (err) return reject(err);
        resolve();
      }
    );
  });

  const shards = [];
  for (let i = 0; i < k; i++) {
    const shardData = Buffer.alloc(shardSize);
    dataBuffer.copy(shardData, 0, i * shardSize, (i + 1) * shardSize);
    shards.push({ index: i, data: shardData });
  }

  for (let i = 0; i < mVal; i++) {
    const shardData = Buffer.alloc(shardSize);
    parityBuffer.copy(shardData, 0, i * shardSize, (i + 1) * shardSize);
    shards.push({ index: k + i, data: shardData });
  }

  return {
    shards,
    shardSize,
    originalLength,
    k,
    m: mVal
  };
}

/**
 * Reconstructs the original buffer from any k+ shards out of the k+m shards.
 *
 * @param {Array<{index: number, data: Buffer}>|{shards: Array<{index: number, data: Buffer}>, originalLength?: number, k?: number, m?: number}} availableShards - Available shards.
 * @param {number} [originalLength] - Original buffer length to trim to (optional if present in first param).
 * @param {number|{k?: number, m?: number}} [kOrOptions=4] - Number of data shards or options object.
 * @param {number} [m=2] - Number of parity shards.
 * @returns {Promise<Buffer>} The reconstructed original buffer.
 */
async function decodeShards(availableShards, originalLength, kOrOptions = K, m = M) {
  let shardsList;
  let targetLength = originalLength;
  let k = K;
  let mVal = M;

  if (typeof kOrOptions === 'object' && kOrOptions !== null) {
    k = typeof kOrOptions.k === 'number' ? kOrOptions.k : K;
    mVal = typeof kOrOptions.m === 'number' ? kOrOptions.m : M;
  } else if (typeof kOrOptions === 'number') {
    k = kOrOptions;
    if (typeof m === 'number') {
      mVal = m;
    }
  }

  if (Array.isArray(availableShards)) {
    shardsList = availableShards;
    if (targetLength === undefined && availableShards.originalLength !== undefined) {
      targetLength = availableShards.originalLength;
    }
  } else if (availableShards && Array.isArray(availableShards.shards)) {
    shardsList = availableShards.shards;
    if (targetLength === undefined) {
      targetLength = availableShards.originalLength;
    }
    if (typeof availableShards.k === 'number') k = availableShards.k;
    if (typeof availableShards.m === 'number') mVal = availableShards.m;
  } else {
    throw new TypeError('availableShards must be an array of shards or an object containing a shards array');
  }

  if (!shardsList || shardsList.length < k) {
    throw new Error(`Insufficient shards: received ${shardsList ? shardsList.length : 0}, but at least ${k} are required`);
  }

  const shardMap = new Map();
  let shardSize = null;

  for (const shard of shardsList) {
    if (!shard || typeof shard.index !== 'number' || !Buffer.isBuffer(shard.data)) {
      throw new TypeError('Each shard must be an object with numeric index and Buffer data');
    }
    if (shard.index < 0 || shard.index >= k + mVal) {
      throw new RangeError(`Shard index ${shard.index} out of valid range [0, ${k + mVal - 1}]`);
    }
    if (shardSize === null) {
      shardSize = shard.data.length;
    } else if (shard.data.length !== shardSize) {
      throw new Error(`Inconsistent shard size: expected ${shardSize}, got ${shard.data.length}`);
    }
    if (!shardMap.has(shard.index)) {
      shardMap.set(shard.index, shard.data);
    }
  }

  if (shardMap.size < k) {
    throw new Error(`Insufficient unique shards: have ${shardMap.size} unique shards, but need at least ${k}`);
  }

  const bufferSize = shardSize * k;
  const paritySize = shardSize * mVal;

  const dataBuffer = Buffer.alloc(bufferSize);
  const parityBuffer = Buffer.alloc(paritySize);

  let sources = 0;
  for (const [index, data] of shardMap.entries()) {
    sources |= (1 << index);
    if (index < k) {
      data.copy(dataBuffer, index * shardSize);
    } else {
      data.copy(parityBuffer, (index - k) * shardSize);
    }
  }

  let targets = 0;
  for (let i = 0; i < k; i++) {
    if (!shardMap.has(i)) {
      targets |= (1 << i);
    }
  }

  if (targets !== 0) {
    const context = ReedSolomon.create(k, mVal);

    await new Promise((resolve, reject) => {
      ReedSolomon.encode(
        context,
        sources,
        targets,
        dataBuffer,
        0,
        bufferSize,
        parityBuffer,
        0,
        paritySize,
        (err) => {
          if (err) return reject(err);
          resolve();
        }
      );
    });
  }

  const fullReconstructed = dataBuffer;

  if (typeof targetLength === 'number' && targetLength >= 0 && targetLength <= bufferSize) {
    return fullReconstructed.subarray(0, targetLength);
  }

  return fullReconstructed;
}

module.exports = {
  K,
  M,
  calculateShardSize,
  encodeBuffer,
  decodeShards
};
