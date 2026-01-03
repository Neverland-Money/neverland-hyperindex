import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

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
  const importRegex =
    /import\s*{\s*([^}]+)\s*}\s*from\s*['"]..\/..\/generated(?:\/index\.js)?['"]/g;

  const sources = files
    .map(file => {
      const content = fs.readFileSync(path.join(handlersDir, file), 'utf8');
      for (const match of content.matchAll(importRegex)) {
        const entries = match[1].split(',');
        for (const entry of entries) {
          const trimmed = entry.trim();
          if (!trimmed) continue;
          const aliasMatch = trimmed.match(/^([A-Za-z0-9_]+)\s+as\s+([A-Za-z0-9_]+)$/);
          if (aliasMatch) {
            const original = aliasMatch[1];
            const alias = aliasMatch[2];
            const existing = aliases.get(original) ?? [];
            if (!existing.includes(alias)) {
              aliases.set(original, [...existing, alias]);
            }
          }
        }
      }
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
      const pattern = new RegExp(`${contract}\\.${event}\\.handler\\b`);
      return pattern.test(sources);
    });
    if (!hasHandler) missing.push(`${entry.contract}.${entry.event}`);
  }

  assert.equal(
    missing.length,
    0,
    missing.length ? `Missing handlers: ${missing.join(', ')}` : undefined
  );
});
