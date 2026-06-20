import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { shouldUseEthCalls } from '../handlers/shared';

type ConfigEvent = {
  contract: string;
  event: string;
};

function parseConfigEvents(configText: string): ConfigEvent[] {
  const events: ConfigEvent[] = [];
  let currentContract: string | null = null;

  for (const line of configText.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const contractMatch = trimmed.match(/^- name:\s*([^\s#]+)/);
    if (contractMatch) {
      currentContract = contractMatch[1];
      continue;
    }

    const eventMatch = trimmed.match(/^- event:\s*([^(]+)\(/);
    if (eventMatch && currentContract) {
      events.push({ contract: currentContract, event: eventMatch[1].trim() });
    }
  }

  return events;
}

function loadHandlerSources(): { sources: string; aliases: Map<string, string[]> } {
  const handlersDir = path.join(process.cwd(), 'src', 'handlers');
  const files = fs.readdirSync(handlersDir).filter(file => file.endsWith('.ts'));
  const aliases = new Map<string, string[]>();

  const sources = files
    .map(file => {
      const content = fs.readFileSync(path.join(handlersDir, file), 'utf8');
      return content;
    })
    .join('\n');

  return { sources, aliases };
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

test('every configured event has a handler', () => {
  const configPath = path.join(process.cwd(), 'config.yaml');
  const configText = fs.readFileSync(configPath, 'utf8');
  const events = parseConfigEvents(configText);
  const { sources, aliases } = loadHandlerSources();

  const missing: string[] = [];
  for (const entry of events) {
    const prefixes = [entry.contract, ...(aliases.get(entry.contract) ?? [])];
    const event = escapeRegExp(entry.event);
    const hasHandler = prefixes.some(prefix => {
      const contract = escapeRegExp(prefix);
      const legacyPattern = new RegExp(`${contract}\\.${event}\\.handler\\b`);
      const v3Pattern = new RegExp(
        `indexer\\.onEvent\\(\\s*\\{\\s*contract:\\s*['"]${contract}['"]\\s*,\\s*event:\\s*['"]${event}['"]`
      );
      return legacyPattern.test(sources) || v3Pattern.test(sources);
    });
    if (!hasHandler) missing.push(`${entry.contract}.${entry.event}`);
  }

  assert.equal(
    missing.length,
    0,
    missing.length ? `Missing handlers: ${missing.join(', ')}` : undefined
  );
});

test('special edition registry config has an explicit production preflight', () => {
  const configText = fs.readFileSync(path.join(process.cwd(), 'config.yaml'), 'utf8');
  const scriptText = fs.readFileSync(
    path.join(process.cwd(), 'scripts', 'check-special-edition-config.ts'),
    'utf8'
  );
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'package.json'), 'utf8')
  ) as { scripts?: Record<string, string> };

  assert.match(configText, /- name:\s*SpecialEditionRegistry/);
  assert.match(configText, /- name:\s*SpecialEditionRegistry[\s\S]*?start_block:\s*82180215/);
  assert.match(scriptText, /ZERO_ADDRESS/);
  assert.match(scriptText, /address\.toLowerCase\(\)/);
  assert.match(scriptText, /ZERO_ADDRESS/);
  assert.match(scriptText, /\^\[1-9\]\[0-9\]\*\$/);
  assert.match(scriptText, /\$\{CONTRACT_NAME\} start_block must be the registry deployment block/);
  assert.equal(
    packageJson.scripts?.['check:special-edition-config'],
    'tsx scripts/check-special-edition-config.ts'
  );
});

test('nft transfer handlers settle before collection ownership mutation', () => {
  const source = fs.readFileSync(path.join(process.cwd(), 'src', 'handlers', 'nft.ts'), 'utf8');
  const settleMatches = [...source.matchAll(/await settlePointsForUser\(/g)];
  const ownershipWriteMatches = [
    ...source.matchAll(/writeOwnership\(\);\n\n\s+\/\/ Update state first/g),
  ];

  assert.equal(settleMatches.length, 2);
  assert.equal(ownershipWriteMatches.length, 2);
  for (let i = 0; i < settleMatches.length; i++) {
    assert.ok(
      (settleMatches[i]?.index ?? 0) < (ownershipWriteMatches[i]?.index ?? 0),
      `NFT transfer path ${i + 1} must settle points before writing ownership`
    );
  }
});

test('special edition membership changes settle owner before count mutation', () => {
  const source = fs.readFileSync(
    path.join(process.cwd(), 'src', 'handlers', 'specialEditions.ts'),
    'utf8'
  );
  const start = source.indexOf('async function applyMembershipChange');
  const end = source.indexOf('export async function handleDustLockSpecialEditionTransfer');
  const membershipSource = source.slice(start, end);
  const settleIndex = membershipSource.indexOf('await settleOwnerBeforeSpecialEditionChange');
  const tokenStateWriteIndex = membershipSource.indexOf('context.SpecialEditionTokenState.set');
  const userDeltaIndex = membershipSource.indexOf('await applyUserSpecialEditionDelta');

  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  assert.notEqual(settleIndex, -1);
  assert.notEqual(tokenStateWriteIndex, -1);
  assert.notEqual(userDeltaIndex, -1);
  assert.ok(
    settleIndex < tokenStateWriteIndex && settleIndex < userDeltaIndex,
    'special edition membership changes must settle before token/user counts change'
  );
});

test('production handlers do not use eth_call or archive-node RPC paths', () => {
  const previousExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const previousEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'true';

  try {
    assert.equal(
      shouldUseEthCalls(),
      false,
      'ENVIO_ENABLE_ETH_CALLS must not enable eth_call paths on Monad'
    );
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = previousExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = previousEth;
  }

  const handlersDir = path.join(process.cwd(), 'src', 'handlers');
  const files = fs.readdirSync(handlersDir).filter(file => file.endsWith('.ts'));
  const violations: string[] = [];
  const forbiddenPatterns: Array<[RegExp, string]> = [
    [/helpers\/viem/, 'imports viem RPC helpers'],
    [/\bpublicClient\b/, 'references publicClient'],
    [/\breadContract\s*\(/, 'uses readContract'],
    [/\bgetBalance\s*\(/, 'uses getBalance'],
    [/\bgetCode\s*\(/, 'uses getCode'],
    [/\bgetStorageAt\s*\(/, 'uses getStorageAt'],
    [/\bgetLogs\s*\(/, 'uses getLogs'],
    [/ENVIO_ENABLE_ETH_CALLS/, 'references ENVIO_ENABLE_ETH_CALLS'],
  ];

  for (const file of files) {
    const source = fs.readFileSync(path.join(handlersDir, file), 'utf8');
    for (const [pattern, reason] of forbiddenPatterns) {
      if (pattern.test(source)) {
        violations.push(`${file}: ${reason}`);
      }
    }
  }

  assert.deepEqual(violations, []);
});
