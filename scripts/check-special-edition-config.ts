import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const CONTRACT_NAME = 'SpecialEditionRegistry';

function extractContractBlock(configText: string, contractName: string): string {
  const lines = configText.split('\n');
  const start = lines.findIndex(line =>
    new RegExp(`^\\s*- name:\\s*${contractName}\\s*$`).test(line)
  );
  assert.notEqual(start, -1, `${contractName} is missing from config.yaml`);

  const blockLines = [lines[start]];
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i];
    if (/^\s{6}- name:\s+/.test(line)) break;
    blockLines.push(line);
  }

  return blockLines.join('\n');
}

function readScalar(block: string, key: string): string | null {
  const match = block.match(new RegExp(`^\\s*${key}:\\s*"?([^"\\s#]+)"?`, 'm'));
  return match?.[1] ?? null;
}

const configText = readFileSync(join(process.cwd(), 'config.yaml'), 'utf8');
const block = extractContractBlock(configText, CONTRACT_NAME);
const address = readScalar(block, 'address');
const startBlock = readScalar(block, 'start_block');

assert.ok(address, `${CONTRACT_NAME} address is missing`);
assert.match(
  address,
  /^0x[0-9a-fA-F]{40}$/,
  `${CONTRACT_NAME} address must be a concrete EVM address`
);
assert.notEqual(
  address.toLowerCase(),
  ZERO_ADDRESS,
  `${CONTRACT_NAME} address must not be the zero placeholder`
);

assert.ok(startBlock, `${CONTRACT_NAME} start_block is missing`);
assert.match(
  startBlock,
  /^[1-9][0-9]*$/,
  `${CONTRACT_NAME} start_block must be the registry deployment block`
);

console.log(
  `[special-edition-config] ${CONTRACT_NAME} address=${address} start_block=${startBlock}`
);
