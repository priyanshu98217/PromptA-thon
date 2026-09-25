'use strict';

const assert = require('assert');
const crypto = require('crypto');
const { encodeBuffer, decodeShards } = require('./erasure.js');

/**
 * Helper to generate all combinations of choosing r elements from an array.
 */
function combinations(arr, r) {
  if (r === 0) return [[]];
  if (arr.length < r) return [];
  const [head, ...tail] = arr;
  const withHead = combinations(tail, r - 1).map((combo) => [head, ...combo]);
  const withoutHead = combinations(tail, r);
  return [...withHead, ...withoutHead];
}

async function runTests() {
  console.log('--- Starting Reed-Solomon Erasure Coding Tests ---\n');

  // Test 1: Basic test as requested (encodes sample buffer, simulates 2 missing shards, asserts result)
  {
    console.log('Test 1: Encoding sample text buffer and simulating 2 missing shards...');
    const originalText = 'Hello, Reed-Solomon Erasure Coding! This is a test message to demonstrate data reconstruction with k=4, m=2.';
    const originalBuffer = Buffer.from(originalText, 'utf8');

    // 1. Encode buffer
    const encoded = await encodeBuffer(originalBuffer);
    console.log(`  Encoded original length: ${encoded.originalLength} bytes`);
    console.log(`  Shard size: ${encoded.shardSize} bytes`);
    console.log(`  Total shards generated: ${encoded.shards.length} (4 data + 2 parity)`);

    assert.strictEqual(encoded.shards.length, 6, 'Should generate 6 shards (4 data + 2 parity)');
    assert.strictEqual(encoded.originalLength, originalBuffer.length, 'originalLength should match input buffer length');
    assert.strictEqual(encoded.shardSize % 8, 0, 'shardSize must be a multiple of 8');

    // 2. Simulate 2 missing shards (e.g. drop shard 0 and shard 1)
    const missingIndices = [0, 1];
    const availableShards = encoded.shards.filter((s) => !missingIndices.includes(s.index));
    console.log(`  Simulating loss of shards ${JSON.stringify(missingIndices)} (remaining: ${availableShards.map(s => s.index).join(', ')})`);

    // 3. Decode
    const reconstructed = await decodeShards(availableShards, encoded.originalLength);
    console.log(`  Reconstructed length: ${reconstructed.length} bytes`);

    // 4. Assert
    assert.strictEqual(
      reconstructed.toString('utf8'),
      originalText,
      'Reconstructed string should match original text'
    );
    assert.deepStrictEqual(
      reconstructed,
      originalBuffer,
      'Reconstructed buffer should exactly match original buffer'
    );
    console.log('  ✓ Test 1 Passed successfully!\n');
  }

  // Test 2: Test all (6 choose 4) = 15 combinations of 2 missing shards
  {
    console.log('Test 2: Testing all 15 possible combinations of 4 available shards (any 2 shards missing)...');
    const samplePayload = crypto.randomBytes(350); // 350 bytes (non-aligned size)
    const encoded = await encodeBuffer(samplePayload);

    const allIndices = [0, 1, 2, 3, 4, 5];
    const all4Combos = combinations(allIndices, 4);

    let passCount = 0;
    for (const combo of all4Combos) {
      const selectedShards = encoded.shards.filter((s) => combo.includes(s.index));
      const missing = allIndices.filter((idx) => !combo.includes(idx));

      const decoded = await decodeShards(selectedShards, encoded.originalLength);
      assert.deepStrictEqual(
        decoded,
        samplePayload,
        `Failed to decode with available shards: ${combo.join(', ')} (missing: ${missing.join(', ')})`
      );
      passCount++;
    }
    console.log(`  ✓ All ${passCount} / 15 combinations of missing 2 shards successfully reconstructed!\n`);
  }

  // Test 3: Test with 5 available shards (1 missing shard)
  {
    console.log('Test 3: Testing with 5 available shards (1 missing shard)...');
    const samplePayload = Buffer.from('Testing single shard loss recovery across all shards.');
    const encoded = await encodeBuffer(samplePayload);

    for (let missingIdx = 0; missingIdx < 6; missingIdx++) {
      const available = encoded.shards.filter((s) => s.index !== missingIdx);
      const decoded = await decodeShards(available, encoded.originalLength);
      assert.deepStrictEqual(decoded, samplePayload, `Failed to reconstruct when shard ${missingIdx} is missing`);
    }
    console.log('  ✓ All 6 single-shard-loss scenarios successfully reconstructed!\n');
  }

  // Test 4: Passing object { shards, originalLength } to decodeShards
  {
    console.log('Test 4: Testing decodeShards with object input format { shards, originalLength }...');
    const payload = Buffer.from('Convenience object payload for decodeShards');
    const encoded = await encodeBuffer(payload);

    // Drop parity shard 4 and data shard 2
    const survivingShards = encoded.shards.filter((s) => s.index !== 2 && s.index !== 4);
    const decoded = await decodeShards({
      shards: survivingShards,
      originalLength: encoded.originalLength
    });

    assert.deepStrictEqual(decoded, payload);
    console.log('  ✓ Object input format passed!\n');
  }

  // Test 5: Various buffer sizes (small, prime, large, exact multiples of shard size)
  {
    console.log('Test 5: Testing edge-case buffer sizes (1 byte, 7 bytes, 32 bytes, 10000 bytes)...');
    const sizes = [1, 7, 8, 15, 31, 32, 33, 100, 1024, 10000];

    for (const size of sizes) {
      const buffer = crypto.randomBytes(size);
      const encoded = await encodeBuffer(buffer);

      // Simulate missing shard 0 and shard 5
      const available = encoded.shards.filter(s => s.index !== 0 && s.index !== 5);
      const decoded = await decodeShards(available, encoded.originalLength);

      assert.strictEqual(decoded.length, size, `Length mismatch for size ${size}`);
      assert.deepStrictEqual(decoded, buffer, `Data mismatch for size ${size}`);
    }
    console.log(`  ✓ All ${sizes.length} buffer sizes passed successfully!\n`);
  }

  // Test 6: Error handling (less than 4 shards, invalid shard indices, etc.)
  {
    console.log('Test 6: Testing error handling for invalid arguments and insufficient shards...');
    const payload = Buffer.from('Error handling test');
    const encoded = await encodeBuffer(payload);

    // Only 3 shards (cannot reconstruct 4 data shards with only 3 shards)
    const only3 = encoded.shards.slice(0, 3);
    await assert.rejects(
      async () => {
        await decodeShards(only3, encoded.originalLength);
      },
      /Insufficient shards/,
      'Should reject when fewer than 4 shards are provided'
    );

    // Invalid shard format
    await assert.rejects(
      async () => {
        await decodeShards([null, undefined, 1, 2]);
      },
      /TypeError/,
      'Should reject invalid shard format'
    );

    console.log('  ✓ Error handling assertions passed!\n');
  }

  console.log('====================================================');
  console.log('  All tests passed successfully! Erasure coding OK. ');
  console.log('====================================================');
}

runTests().catch((err) => {
  console.error('Test failed with error:', err);
  process.exit(1);
});
