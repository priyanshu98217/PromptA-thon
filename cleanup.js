'use strict';

const fs = require('fs');
const path = require('path');

const NODES_DIR = path.join(__dirname, 'nodes');
const METADATA_DIR = path.join(__dirname, 'metadata');

console.log('--- Cleaning up storage and metadata ---');

// 1. Clean nodes directory
if (fs.existsSync(NODES_DIR)) {
  const entries = fs.readdirSync(NODES_DIR);
  for (const entry of entries) {
    const fullPath = path.join(NODES_DIR, entry);
    // If it is a down folder (e.g. node0_DOWN), rename it back or delete it
    if (entry.includes('_DOWN')) {
      const normalName = entry.replace('_DOWN', '');
      const normalPath = path.join(NODES_DIR, normalName);
      if (fs.existsSync(normalPath)) {
        fs.rmSync(fullPath, { recursive: true, force: true });
      } else {
        fs.renameSync(fullPath, normalPath);
      }
    }
  }

  // Ensure node0..node5 exist and are empty, remove extra dynamic nodes
  for (let i = 0; i < 20; i++) {
    const nodePath = path.join(NODES_DIR, `node${i}`);
    if (i < 6) {
      if (!fs.existsSync(nodePath)) {
        fs.mkdirSync(nodePath, { recursive: true });
      } else {
        const files = fs.readdirSync(nodePath);
        for (const file of files) {
          fs.unlinkSync(path.join(nodePath, file));
        }
      }
    } else {
      if (fs.existsSync(nodePath)) {
        fs.rmSync(nodePath, { recursive: true, force: true });
      }
    }
  }
  console.log('✓ Cleaned and restored nodes/node0 through nodes/node5 (empty folders)');
}

// 2. Clean metadata directory
if (fs.existsSync(METADATA_DIR)) {
  const files = fs.readdirSync(METADATA_DIR);
  for (const file of files) {
    fs.unlinkSync(path.join(METADATA_DIR, file));
  }
  console.log('✓ Cleaned metadata/ folder (empty)');
} else {
  fs.mkdirSync(METADATA_DIR, { recursive: true });
  console.log('✓ Created empty metadata/ folder');
}

console.log('--- Storage cleanup complete ---');
