#!/usr/bin/env tsx
/* c8 ignore file -- rollout CLI is exercised through deterministic offline fixtures. */
/**
 * Strict leaderboard rollout comparator.
 *
 * Default mode performs read-only, id-cursor-paginated GraphQL reads from the
 * configured baseline and candidate endpoints. Offline mode never opens a
 * network connection:
 *
 *   pnpm exec tsx scripts/compare-leaderboard-parity.ts --fixture report.json
 *
 * Live inputs:
 *   PROD_GRAPHQL_URL / PROD_ADMIN_SECRET       baseline endpoint
 *   LOCAL_GRAPHQL_URL / LOCAL_ADMIN_SECRET     candidate endpoint
 *   PARITY_EXPECTED_TARGET_BLOCK               required fixed target on both endpoints
 *   PARITY_REQUEST_TIMEOUT_MS                  optional, default 10,000 (max 120,000)
 *   PARITY_OVERALL_TIMEOUT_MS                  optional, default 120,000 (max 600,000)
 *   PARITY_MAX_PAGES / PARITY_MAX_ROWS         optional bounded collection ceilings
 *   PARITY_LP_CLASSIFICATIONS_PATH             optional JSON classification file
 *   PARITY_ARCHIVED_MANIFEST_PATH              required when a floor is proposed
 *   PARITY_PROPOSED_FINAL_ONLY_FLOOR            proposed Tide floor
 *
 * Classification file:
 *   { "rows": [{ "user", "epochNumber", "component",
 *      "computedRoundingBound", "kind"?: "MULTIPLIER_SEMANTIC", "note"?: string }] }
 *
 * Archived manifest file:
 *   { "tides": [{ "epochNumber": "8", "users": ["0x..."] }] }
 */

import { readFileSync } from 'node:fs';

type Endpoint = { name: string; url: string; secret: string };
type LiveRuntime = {
  baseline: Endpoint;
  candidate: Endpoint;
  expectedTargetBlock: string;
  requestTimeoutMs: number;
  overallDeadlineAt: number;
  maxPages: number;
  maxRows: number;
};
export type ExactScalar = string | number | null;
export type ParityEntity = 'UserPoints' | 'UserLeaderboardState' | 'UserEpochStats';
export type LPComponent =
  | 'lpPoints'
  | 'dailyLPPoints'
  | 'lpPointsWithMultiplier'
  | 'totalPoints'
  | 'totalPointsWithMultiplier'
  | 'lifetimeTotalPoints'
  | 'lifetimePoints';

export type ParityFieldRule = {
  field: string;
  category: 'IDENTITY' | 'EXACT' | 'LP_BEARING' | 'EXCLUDED';
  valueType:
    | 'ADDRESS'
    | 'ID'
    | 'BIGINT'
    | 'NULLABLE_BIGINT'
    | 'INT'
    | 'NULLABLE_INT'
    | 'STRING'
    | 'NULLABLE_STRING'
    | 'BIGINT_SET';
  comparison?: 'DRIFT' | 'CONSISTENCY';
  reason?: string;
};

export const LEADERBOARD_PARITY_FIELD_MANIFEST: Record<ParityEntity, readonly ParityFieldRule[]> = {
  UserPoints: [
    { field: 'id', category: 'IDENTITY', valueType: 'ID' },
    { field: 'user_id', category: 'IDENTITY', valueType: 'ADDRESS' },
    { field: 'lifetimeDepositPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeBorrowPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeDailySupplyPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeDailyBorrowPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeDailyRepayPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeDailyWithdrawPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lifetimeDailyVPPoints', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'lifetimeTotalPoints',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    { field: 'epochsParticipated', category: 'EXACT', valueType: 'BIGINT_SET' },
    { field: 'lifetimeEpochsIncluded', category: 'EXACT', valueType: 'BIGINT_SET' },
    {
      field: 'lastUpdatedAt',
      category: 'EXCLUDED',
      valueType: 'INT',
      reason: 'Operational write time can move when lazy settlement defers a user touch.',
    },
  ],
  UserLeaderboardState: [
    { field: 'id', category: 'IDENTITY', valueType: 'ID' },
    { field: 'user_id', category: 'IDENTITY', valueType: 'ADDRESS' },
    { field: 'nftCount', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'nftMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'specialEditionCount', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'specialEditionMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'votingPower', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'vpTierIndex', category: 'EXACT', valueType: 'NULLABLE_BIGINT' },
    { field: 'vpMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'combinedMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'totalEpochsParticipated', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'lifetimePoints',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    { field: 'currentEpochId', category: 'EXACT', valueType: 'NULLABLE_STRING' },
    {
      field: 'currentEpochRank',
      category: 'EXCLUDED',
      valueType: 'NULLABLE_BIGINT',
      reason: 'Approximate materialized rank is superseded by exact closed-Tide rank cohorts.',
    },
    {
      field: 'lastUpdate',
      category: 'EXCLUDED',
      valueType: 'INT',
      reason: 'Operational activity watermark is checked separately and write timing may differ.',
    },
  ],
  UserEpochStats: [
    { field: 'id', category: 'IDENTITY', valueType: 'ID' },
    { field: 'user_id', category: 'IDENTITY', valueType: 'ADDRESS' },
    { field: 'epochNumber', category: 'IDENTITY', valueType: 'BIGINT' },
    { field: 'depositPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'borrowPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'lpPoints', category: 'LP_BEARING', valueType: 'BIGINT', comparison: 'DRIFT' },
    { field: 'dailySupplyPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'dailyBorrowPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'dailyRepayPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'dailyWithdrawPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'dailyVPPoints', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'dailyLPPoints',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    { field: 'manualAwardPoints', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'depositMultiplierBps', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'borrowMultiplierBps', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'vpMultiplierBps', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'lpMultiplierBps',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'CONSISTENCY',
    },
    { field: 'depositPointsWithMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'borrowPointsWithMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    { field: 'vpPointsWithMultiplier', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'lpPointsWithMultiplier',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    { field: 'lastSupplyPointsDay', category: 'EXACT', valueType: 'INT' },
    { field: 'lastBorrowPointsDay', category: 'EXACT', valueType: 'INT' },
    { field: 'lastRepayPointsDay', category: 'EXACT', valueType: 'INT' },
    { field: 'lastWithdrawPointsDay', category: 'EXACT', valueType: 'INT' },
    { field: 'lastVPPointsDay', category: 'EXACT', valueType: 'INT' },
    { field: 'lastVPAccrualTimestamp', category: 'EXACT', valueType: 'INT' },
    {
      field: 'totalPoints',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    {
      field: 'totalPointsWithMultiplier',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'DRIFT',
    },
    {
      field: 'totalMultiplierBps',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'CONSISTENCY',
    },
    {
      field: 'lastAppliedMultiplierBps',
      category: 'LP_BEARING',
      valueType: 'BIGINT',
      comparison: 'CONSISTENCY',
    },
    { field: 'testnetBonusBps', category: 'EXACT', valueType: 'BIGINT' },
    {
      field: 'rank',
      category: 'EXCLUDED',
      valueType: 'NULLABLE_INT',
      reason: 'Approximate materialized rank is superseded by exact closed-Tide rank cohorts.',
    },
    {
      field: 'firstSeenAt',
      category: 'EXCLUDED',
      valueType: 'INT',
      reason: 'Operational creation time can move when lazy settlement first materializes the row.',
    },
    {
      field: 'lastUpdatedAt',
      category: 'EXCLUDED',
      valueType: 'INT',
      reason: 'Operational write time can move when lazy settlement defers a user touch.',
    },
  ],
};

export type NonLPDifference = {
  user?: string;
  epochNumber?: ExactScalar;
  field: string;
  baseline: ExactScalar;
  candidate: ExactScalar;
};

export type LPDifferenceInput = {
  user: string;
  epochNumber: ExactScalar;
  component: LPComponent;
  material?: ParityEntity;
  baselinePoints: ExactScalar;
  candidatePoints: ExactScalar;
  computedRoundingBound?: ExactScalar;
  kind?: 'MULTIPLIER_SEMANTIC';
  note?: string;
};

export type ClosedTideRankingInput = {
  epochNumber: ExactScalar;
  baselineScores: ClosedTideScoreInput[];
  candidateScores: ClosedTideScoreInput[];
};

export type ClosedTideScoreInput = {
  user: string;
  totalPoints: ExactScalar;
  totalPointsWithMultiplier: ExactScalar;
};

export type FinalizationTideInput = {
  epochNumber: ExactScalar;
  activePhaseUsers: string[];
  finalizationUsers?: string[];
};

export type ManifestTideInput = { epochNumber: ExactScalar; users: string[] };

export type ParityEvaluationInput = {
  exactScalarPrecision: boolean;
  precisionFailures?: string[];
  preconditionFailures?: string[];
  nonLpDifferences: NonLPDifference[];
  lpDifferences: LPDifferenceInput[];
  closedTideRankings: ClosedTideRankingInput[];
  finalizationCoverage?: {
    proposedFloor: ExactScalar;
    baselineClosedTides?: ExactScalar[];
    candidateClosedTides?: ExactScalar[];
    requiredClosedTides: ExactScalar[];
    tides: FinalizationTideInput[];
    manifestTides?: ManifestTideInput[];
  };
};

export type EvaluatedLPDifference = {
  user: string;
  epochNumber: string;
  component: LPComponent;
  material: ParityEntity;
  baselinePoints: string;
  candidatePoints: string;
  integerDrift: string;
  percentageDrift: string;
  computedRoundingBound?: string;
  classification:
    | 'NO_DRIFT'
    | 'WITHIN_COMPUTED_BOUND'
    | 'MULTIPLIER_SEMANTIC'
    | 'UNCLASSIFIED_OR_OUT_OF_BOUND'
    | 'SCALAR_PRECISION_UNAVAILABLE';
  note?: string;
};

export type EvaluatedFinalizationTide = {
  epochNumber: string;
  activePhaseUsers: string[];
  missingManifestUsers: string[];
  missingFinalizationUsers: string[];
  manifestMaterialPresent: boolean;
  finalizationMaterialPresent: boolean;
};

export type ParityEvaluation = {
  pass: boolean;
  verdict: 'PASS' | 'BLOCK';
  blockers: string[];
  nonLpDifferences: NonLPDifference[];
  lpDifferences: EvaluatedLPDifference[];
  rankChanges: {
    epochNumber: string;
    winnerChanged: boolean;
    baselineOrder: string[];
    candidateOrder: string[];
    baselineCohorts: Array<{ rank: number; users: string[] }>;
    candidateCohorts: Array<{ rank: number; users: string[] }>;
  }[];
  finalizationCoverage: EvaluatedFinalizationTide[];
};

const LIVE_REQUEST_TIMEOUT_MS = 10_000;
const LIVE_OVERALL_TIMEOUT_MS = 120_000;
const LIVE_MAX_PAGES = 1_000;
const LIVE_MAX_ROWS = 250_000;

function positiveBoundedEnvironmentInteger(
  name: string,
  fallback: number | undefined,
  maximum: number
): number {
  const raw = process.env[name]?.trim();
  if (!raw) {
    if (fallback !== undefined) return fallback;
    throw new Error(`${name} is required and must be a positive integer`);
  }
  if (!/^[1-9][0-9]*$/.test(raw)) {
    throw new Error(`${name} must be a positive integer no greater than ${maximum}`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value > maximum) {
    throw new Error(`${name} must be a positive integer no greater than ${maximum}`);
  }
  return value;
}

function explicitGraphQLURL(name: string): string {
  const raw = process.env[name]?.trim();
  if (!raw) throw new Error(`${name} is required for live parity mode`);
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`${name} must be a valid HTTP(S) URL`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`${name} must be a valid HTTP(S) URL`);
  }
  parsed.hash = '';
  if (parsed.pathname !== '/') parsed.pathname = parsed.pathname.replace(/\/+$/, '');
  return parsed.toString();
}

function adminSecretFromEnvironment(name: 'PROD_ADMIN_SECRET' | 'LOCAL_ADMIN_SECRET'): string {
  const value = process.env[name] ?? '';
  if (!value) return '';
  if (/[\u0000-\u001f\u007f-\u009f]/.test(value)) {
    throw new Error(`${name} contains an invalid HTTP header value`);
  }
  try {
    const headers = new Headers();
    headers.set('x-hasura-admin-secret', value);
    if (headers.get('x-hasura-admin-secret') !== value) {
      throw new Error('header normalization changed the configured value');
    }
  } catch {
    throw new Error(`${name} contains an invalid HTTP header value`);
  }
  return value;
}

function liveRuntimeFromEnvironment(): LiveRuntime {
  const baselineURL = explicitGraphQLURL('PROD_GRAPHQL_URL');
  const candidateURL = explicitGraphQLURL('LOCAL_GRAPHQL_URL');
  if (baselineURL === candidateURL) {
    throw new Error('PROD_GRAPHQL_URL and LOCAL_GRAPHQL_URL must be distinct normalized URLs');
  }
  const expectedTargetBlock = positiveBoundedEnvironmentInteger(
    'PARITY_EXPECTED_TARGET_BLOCK',
    undefined,
    Number.MAX_SAFE_INTEGER
  ).toString();
  const requestTimeoutMs = positiveBoundedEnvironmentInteger(
    'PARITY_REQUEST_TIMEOUT_MS',
    LIVE_REQUEST_TIMEOUT_MS,
    120_000
  );
  const overallTimeoutMs = positiveBoundedEnvironmentInteger(
    'PARITY_OVERALL_TIMEOUT_MS',
    LIVE_OVERALL_TIMEOUT_MS,
    600_000
  );
  return {
    baseline: {
      name: 'baseline',
      url: baselineURL,
      secret: adminSecretFromEnvironment('PROD_ADMIN_SECRET'),
    },
    candidate: {
      name: 'candidate',
      url: candidateURL,
      secret: adminSecretFromEnvironment('LOCAL_ADMIN_SECRET'),
    },
    expectedTargetBlock,
    requestTimeoutMs,
    overallDeadlineAt: Date.now() + overallTimeoutMs,
    maxPages: positiveBoundedEnvironmentInteger('PARITY_MAX_PAGES', LIVE_MAX_PAGES, 10_000),
    maxRows: positiveBoundedEnvironmentInteger('PARITY_MAX_ROWS', LIVE_MAX_ROWS, 1_000_000),
  };
}

function normalizeUser(user: string): string {
  return user.trim().toLowerCase();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isAddress(value: unknown): value is string {
  return typeof value === 'string' && /^0x[0-9a-f]{40}$/i.test(value);
}

function isExactInteger(value: unknown): value is string {
  return typeof value === 'string' && /^-?[0-9]+$/.test(value);
}

function isNonnegativeExactInteger(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9]+$/.test(value);
}

function inputFailure(failures: string[], path: string, reason: string): void {
  failures.push(`INPUT_INVALID ${path}: ${reason}`);
}

function validatedStringArray(
  value: unknown,
  path: string,
  failures: string[],
  options: { addresses?: boolean; allowDuplicateAddresses?: boolean } = {}
): string[] {
  if (!Array.isArray(value)) {
    inputFailure(failures, path, 'expected array');
    return [];
  }
  const result: string[] = [];
  const seen = new Set<string>();
  for (const [index, entry] of value.entries()) {
    if (typeof entry !== 'string' || (options.addresses && !isAddress(entry))) {
      inputFailure(
        failures,
        `${path}[${index}]`,
        options.addresses ? 'expected address' : 'expected string'
      );
      continue;
    }
    const normalized = options.addresses ? normalizeUser(entry) : entry;
    if (seen.has(normalized) && !options.allowDuplicateAddresses) {
      const source = path.includes('manifest')
        ? 'manifest '
        : path.includes('finalization')
          ? 'finalization '
          : '';
      inputFailure(
        failures,
        path,
        `DUPLICATE ${source}${options.addresses ? 'user' : 'value'} ${normalized}`
      );
      continue;
    }
    seen.add(normalized);
    result.push(entry);
  }
  return result;
}

function validatedExactScalar(
  value: unknown,
  path: string,
  failures: string[],
  nonnegative = true
): string | undefined {
  const valid = nonnegative ? isNonnegativeExactInteger(value) : isExactInteger(value);
  if (!valid) {
    failures.push(
      `INPUT_INVALID SCALAR_PRECISION_UNAVAILABLE ${path}: expected exact integer string`
    );
    return undefined;
  }
  return BigInt(value as string).toString();
}

const LP_COMPONENT_SET = new Set<LPComponent>([
  'lpPoints',
  'dailyLPPoints',
  'lpPointsWithMultiplier',
  'totalPoints',
  'totalPointsWithMultiplier',
  'lifetimeTotalPoints',
  'lifetimePoints',
]);
const PARITY_ENTITY_SET = new Set<ParityEntity>([
  'UserPoints',
  'UserLeaderboardState',
  'UserEpochStats',
]);

function componentBelongsToMaterial(component: LPComponent, material: ParityEntity): boolean {
  if (material === 'UserPoints') return component === 'lifetimeTotalPoints';
  if (material === 'UserLeaderboardState') return component === 'lifetimePoints';
  return component !== 'lifetimeTotalPoints' && component !== 'lifetimePoints';
}

function emptyEvaluationInput(): ParityEvaluationInput {
  return {
    exactScalarPrecision: false,
    precisionFailures: [],
    preconditionFailures: [],
    nonLpDifferences: [],
    lpDifferences: [],
    closedTideRankings: [],
  };
}

export function validateParityEvaluationInput(raw: unknown): {
  input: ParityEvaluationInput;
  failures: string[];
} {
  const failures: string[] = [];
  const input = emptyEvaluationInput();
  if (!isRecord(raw)) {
    inputFailure(failures, 'root', 'expected object');
    return { input, failures };
  }

  if (typeof raw.exactScalarPrecision !== 'boolean') {
    inputFailure(failures, 'exactScalarPrecision', 'expected boolean');
  } else {
    input.exactScalarPrecision = raw.exactScalarPrecision;
  }
  if (raw.precisionFailures !== undefined) {
    input.precisionFailures = validatedStringArray(
      raw.precisionFailures,
      'precisionFailures',
      failures
    );
  }
  if (raw.preconditionFailures !== undefined) {
    input.preconditionFailures = validatedStringArray(
      raw.preconditionFailures,
      'preconditionFailures',
      failures
    );
  }

  if (!Array.isArray(raw.nonLpDifferences)) {
    inputFailure(failures, 'nonLpDifferences', 'expected array');
  } else {
    for (const [index, value] of raw.nonLpDifferences.entries()) {
      const path = `nonLpDifferences[${index}]`;
      if (!isRecord(value)) {
        inputFailure(failures, path, 'expected object');
        continue;
      }
      if (value.user !== undefined && !isAddress(value.user)) {
        inputFailure(failures, `${path}.user`, 'expected address');
      }
      if (
        value.epochNumber !== undefined &&
        value.epochNumber !== null &&
        !isNonnegativeExactInteger(value.epochNumber)
      ) {
        inputFailure(
          failures,
          `${path}.epochNumber`,
          'SCALAR_PRECISION_UNAVAILABLE expected nonnegative exact integer string'
        );
      }
      if (typeof value.field !== 'string' || !value.field.trim()) {
        inputFailure(failures, `${path}.field`, 'expected nonempty string');
      }
      const validComparable = (entry: unknown) =>
        entry === null || typeof entry === 'string' || typeof entry === 'number';
      if (!validComparable(value.baseline)) {
        inputFailure(failures, `${path}.baseline`, 'expected scalar or null');
      }
      if (!validComparable(value.candidate)) {
        inputFailure(failures, `${path}.candidate`, 'expected scalar or null');
      }
      if (
        (value.user === undefined || isAddress(value.user)) &&
        typeof value.field === 'string' &&
        validComparable(value.baseline) &&
        validComparable(value.candidate)
      ) {
        input.nonLpDifferences.push(value as NonLPDifference);
      }
    }
  }

  if (!Array.isArray(raw.lpDifferences)) {
    inputFailure(failures, 'lpDifferences', 'expected array');
  } else {
    const seen = new Set<string>();
    for (const [index, value] of raw.lpDifferences.entries()) {
      const path = `lpDifferences[${index}]`;
      if (!isRecord(value)) {
        inputFailure(failures, path, 'expected object');
        continue;
      }
      const material =
        value.material === undefined
          ? 'UserEpochStats'
          : typeof value.material === 'string' &&
              PARITY_ENTITY_SET.has(value.material as ParityEntity)
            ? (value.material as ParityEntity)
            : undefined;
      if (!isAddress(value.user)) inputFailure(failures, `${path}.user`, 'expected address');
      if (material === undefined) inputFailure(failures, `${path}.material`, 'unknown entity');
      const component =
        typeof value.component === 'string' && LP_COMPONENT_SET.has(value.component as LPComponent)
          ? (value.component as LPComponent)
          : undefined;
      if (component === undefined) inputFailure(failures, `${path}.component`, 'unknown component');
      if (component && material && !componentBelongsToMaterial(component, material)) {
        inputFailure(failures, `${path}.component`, `not valid for ${material}`);
      }
      const epochNumber = validatedExactScalar(value.epochNumber, `${path}.epochNumber`, failures);
      const baselinePoints = validatedExactScalar(
        value.baselinePoints,
        `${path}.baselinePoints`,
        failures
      );
      const candidatePoints = validatedExactScalar(
        value.candidatePoints,
        `${path}.candidatePoints`,
        failures
      );
      const computedRoundingBound =
        value.computedRoundingBound === undefined
          ? undefined
          : validatedExactScalar(
              value.computedRoundingBound,
              `${path}.computedRoundingBound`,
              failures
            );
      const kind =
        value.kind === undefined || value.kind === 'MULTIPLIER_SEMANTIC' ? value.kind : undefined;
      if (value.kind !== undefined && kind === undefined) {
        inputFailure(failures, `${path}.kind`, 'unknown enum value');
      }
      if (value.note !== undefined && typeof value.note !== 'string') {
        inputFailure(failures, `${path}.note`, 'expected string');
      }
      if (isAddress(value.user) && component && material && epochNumber) {
        const key = `${material}:${normalizeUser(value.user)}:${epochNumber}:${component}`;
        if (seen.has(key)) inputFailure(failures, path, `duplicate LP material key ${key}`);
        seen.add(key);
      }
      if (
        isAddress(value.user) &&
        component &&
        material &&
        componentBelongsToMaterial(component, material) &&
        epochNumber &&
        baselinePoints &&
        candidatePoints &&
        (value.computedRoundingBound === undefined || computedRoundingBound !== undefined) &&
        (value.kind === undefined || kind !== undefined) &&
        (value.note === undefined || typeof value.note === 'string')
      ) {
        input.lpDifferences.push({
          user: value.user,
          epochNumber,
          component,
          material,
          baselinePoints,
          candidatePoints,
          computedRoundingBound,
          kind,
          note: value.note as string | undefined,
        });
      }
    }
  }

  if (!Array.isArray(raw.closedTideRankings)) {
    inputFailure(failures, 'closedTideRankings', 'expected array');
  } else {
    const seenEpochs = new Set<string>();
    for (const [index, value] of raw.closedTideRankings.entries()) {
      const path = `closedTideRankings[${index}]`;
      if (!isRecord(value)) {
        inputFailure(failures, path, 'expected object');
        continue;
      }
      const epochNumber = validatedExactScalar(value.epochNumber, `${path}.epochNumber`, failures);
      if (epochNumber && seenEpochs.has(epochNumber)) {
        inputFailure(failures, path, `duplicate Tide ${epochNumber}`);
      }
      if (epochNumber) seenEpochs.add(epochNumber);
      const validateScores = (rawScores: unknown, side: string): ClosedTideScoreInput[] => {
        if (!Array.isArray(rawScores)) {
          inputFailure(failures, `${path}.${side}Scores`, 'expected array');
          return [];
        }
        const scores: ClosedTideScoreInput[] = [];
        const seenUsers = new Set<string>();
        for (const [scoreIndex, rawScore] of rawScores.entries()) {
          const scorePath = `${path}.${side}Scores[${scoreIndex}]`;
          if (!isRecord(rawScore)) {
            inputFailure(failures, scorePath, 'expected object');
            continue;
          }
          if (!isAddress(rawScore.user)) {
            inputFailure(failures, `${scorePath}.user`, 'expected address');
          }
          const user = isAddress(rawScore.user) ? normalizeUser(rawScore.user) : '';
          if (user && seenUsers.has(user)) {
            inputFailure(failures, `${path}.${side}Scores`, `duplicate user ${user}`);
          }
          if (user) seenUsers.add(user);
          const totalPoints = validatedExactScalar(
            rawScore.totalPoints,
            `${scorePath}.totalPoints`,
            failures
          );
          const totalPointsWithMultiplier = validatedExactScalar(
            rawScore.totalPointsWithMultiplier,
            `${scorePath}.totalPointsWithMultiplier`,
            failures
          );
          if (isAddress(rawScore.user) && totalPoints && totalPointsWithMultiplier) {
            scores.push({ user: rawScore.user, totalPoints, totalPointsWithMultiplier });
          }
        }
        return scores;
      };
      const baselineScores = validateScores(value.baselineScores, 'baseline');
      const candidateScores = validateScores(value.candidateScores, 'candidate');
      if (epochNumber) {
        input.closedTideRankings.push({ epochNumber, baselineScores, candidateScores });
      }
    }
  }

  if (raw.finalizationCoverage !== undefined) {
    if (!isRecord(raw.finalizationCoverage)) {
      inputFailure(failures, 'finalizationCoverage', 'expected object');
    } else {
      const coverage = raw.finalizationCoverage;
      const proposedFloor = validatedExactScalar(
        coverage.proposedFloor,
        'finalizationCoverage.proposedFloor',
        failures
      );
      const scalarArray = (value: unknown, path: string): ExactScalar[] => {
        if (!Array.isArray(value)) {
          inputFailure(failures, path, 'expected array');
          return [];
        }
        const result: string[] = [];
        const seen = new Set<string>();
        for (const [index, entry] of value.entries()) {
          const scalar = validatedExactScalar(entry, `${path}[${index}]`, failures);
          if (!scalar) continue;
          if (seen.has(scalar)) inputFailure(failures, path, `duplicate Tide ${scalar}`);
          seen.add(scalar);
          result.push(scalar);
        }
        return result;
      };
      const requiredClosedTides = scalarArray(
        coverage.requiredClosedTides,
        'finalizationCoverage.requiredClosedTides'
      );
      const baselineClosedTides =
        coverage.baselineClosedTides === undefined
          ? undefined
          : scalarArray(coverage.baselineClosedTides, 'finalizationCoverage.baselineClosedTides');
      const candidateClosedTides =
        coverage.candidateClosedTides === undefined
          ? undefined
          : scalarArray(coverage.candidateClosedTides, 'finalizationCoverage.candidateClosedTides');
      const tides: FinalizationTideInput[] = [];
      const seenCoverageTides = new Set<string>();
      if (!Array.isArray(coverage.tides)) {
        inputFailure(failures, 'finalizationCoverage.tides', 'expected array');
      } else {
        for (const [index, value] of coverage.tides.entries()) {
          const path = `finalizationCoverage.tides[${index}]`;
          if (!isRecord(value)) {
            inputFailure(failures, path, 'expected object');
            continue;
          }
          const epochNumber = validatedExactScalar(
            value.epochNumber,
            `${path}.epochNumber`,
            failures
          );
          if (epochNumber && seenCoverageTides.has(epochNumber)) {
            inputFailure(failures, path, `duplicate Tide ${epochNumber}`);
          }
          if (epochNumber) seenCoverageTides.add(epochNumber);
          const activePhaseUsers = validatedStringArray(
            value.activePhaseUsers,
            `${path}.activePhaseUsers`,
            failures,
            { addresses: true, allowDuplicateAddresses: true }
          );
          const finalizationUsers =
            value.finalizationUsers === undefined
              ? undefined
              : validatedStringArray(
                  value.finalizationUsers,
                  `${path}.finalizationUsers`,
                  failures,
                  { addresses: true }
                );
          if (epochNumber) tides.push({ epochNumber, activePhaseUsers, finalizationUsers });
        }
      }
      const manifestTides: ManifestTideInput[] = [];
      const seenManifestTides = new Set<string>();
      if (coverage.manifestTides !== undefined) {
        if (!Array.isArray(coverage.manifestTides)) {
          inputFailure(failures, 'finalizationCoverage.manifestTides', 'expected array');
        } else {
          for (const [index, value] of coverage.manifestTides.entries()) {
            const path = `finalizationCoverage.manifestTides[${index}]`;
            if (!isRecord(value)) {
              inputFailure(failures, path, 'expected object');
              continue;
            }
            const epochNumber = validatedExactScalar(
              value.epochNumber,
              `${path}.epochNumber`,
              failures
            );
            if (epochNumber && seenManifestTides.has(epochNumber)) {
              inputFailure(failures, path, `duplicate Tide ${epochNumber}`);
            }
            if (epochNumber) seenManifestTides.add(epochNumber);
            const users = validatedStringArray(value.users, `${path}.users`, failures, {
              addresses: true,
            });
            if (epochNumber) manifestTides.push({ epochNumber, users });
          }
        }
      }
      if (proposedFloor) {
        input.finalizationCoverage = {
          proposedFloor,
          requiredClosedTides,
          baselineClosedTides,
          candidateClosedTides,
          tides,
          manifestTides: coverage.manifestTides === undefined ? undefined : manifestTides,
        };
      }
    }
  }
  return { input, failures };
}

function parseExactScalar(
  value: ExactScalar | undefined,
  label: string,
  blockers: string[]
): bigint | undefined {
  if (typeof value !== 'string' || !/^-?[0-9]+$/.test(value)) {
    blockers.push(`SCALAR_PRECISION_UNAVAILABLE ${label}: ${String(value)}`);
    return undefined;
  }
  return BigInt(value);
}

function absBigInt(value: bigint): bigint {
  return value < 0n ? -value : value;
}

function formatPercentage(drift: bigint, baseline: bigint): string {
  if (drift === 0n) return '0.000000%';
  if (baseline === 0n) return drift > 0n ? '+infinite' : '-infinite';
  const sign = drift < 0n ? '-' : '+';
  const scaled = (absBigInt(drift) * 100_000_000n) / absBigInt(baseline);
  const whole = scaled / 1_000_000n;
  const fraction = (scaled % 1_000_000n).toString().padStart(6, '0');
  return `${sign}${whole.toString()}.${fraction}%`;
}

function normalizedSet(users: readonly string[]): Set<string> {
  return new Set(users.map(normalizeUser));
}

type ExactRankingMaterial = {
  order: string[];
  cohorts: Array<{ rank: number; users: string[] }>;
  byUser: Map<string, { rank: number; tieGroup: string }>;
  winners: Set<string>;
};

function exactRankingMaterial(
  scores: readonly ClosedTideScoreInput[],
  label: string,
  blockers: string[]
): ExactRankingMaterial {
  const parsed: Array<{ user: string; score: bigint }> = [];
  const seen = new Set<string>();
  for (const row of scores) {
    const user = normalizeUser(row.user);
    if (!user) {
      blockers.push(`CLOSED_TIDE_SCORE_MATERIAL_INVALID ${label}: empty user`);
      continue;
    }
    if (seen.has(user)) {
      blockers.push(`CLOSED_TIDE_SCORE_MATERIAL_INVALID ${label}: duplicate user ${user}`);
      continue;
    }
    seen.add(user);
    const total = parseExactScalar(row.totalPoints, `${label} ${user} totalPoints`, blockers);
    const score = parseExactScalar(
      row.totalPointsWithMultiplier,
      `${label} ${user} totalPointsWithMultiplier`,
      blockers
    );
    if (total !== undefined && score !== undefined) parsed.push({ user, score });
  }
  parsed.sort((left, right) =>
    left.score === right.score ? 0 : left.score > right.score ? -1 : 1
  );
  const grouped = new Map<string, string[]>();
  for (const row of parsed) {
    const score = row.score.toString();
    const users = grouped.get(score) ?? [];
    users.push(row.user);
    grouped.set(score, users);
  }
  const cohorts: Array<{ rank: number; users: string[] }> = [];
  let precedingUsers = 0;
  for (const users of grouped.values()) {
    const sortedUsers = users.sort();
    cohorts.push({ rank: precedingUsers + 1, users: sortedUsers });
    precedingUsers += users.length;
  }
  const byUser = new Map<string, { rank: number; tieGroup: string }>();
  for (const cohort of cohorts) {
    const tieGroup = cohort.users.join(',');
    for (const user of cohort.users) byUser.set(user, { rank: cohort.rank, tieGroup });
  }
  return {
    order: cohorts.flatMap(cohort => cohort.users),
    cohorts,
    byUser,
    winners: new Set(cohorts[0]?.users ?? []),
  };
}

function sameStringSet(left: Set<string>, right: Set<string>): boolean {
  return left.size === right.size && [...left].every(value => right.has(value));
}

function sameScalarSet(left: readonly bigint[], right: readonly bigint[]): boolean {
  const leftSet = new Set(left.map(String));
  const rightSet = new Set(right.map(String));
  return sameStringSet(leftSet, rightSet);
}

function contiguousRange(floor: bigint, maximum: bigint, blockers: string[]): bigint[] {
  if (maximum < floor) return [];
  if (maximum - floor > 10_000n) {
    blockers.push(`CLOSED_TIDE_RANGE_INVALID floor=${floor} maximum=${maximum}`);
    return [];
  }
  const range: bigint[] = [];
  for (let epoch = floor; epoch <= maximum; epoch += 1n) range.push(epoch);
  return range;
}

function describeSetDifference(expected: readonly bigint[], actual: readonly bigint[]): string {
  const expectedSet = new Set(expected.map(String));
  const actualSet = new Set(actual.map(String));
  const missing = [...expectedSet].filter(value => !actualSet.has(value));
  const extra = [...actualSet].filter(value => !expectedSet.has(value));
  return `missing=[${missing.join(',')}] extra=[${extra.join(',')}]`;
}

export function evaluateParityReport(rawInput: unknown): ParityEvaluation {
  const validation = validateParityEvaluationInput(rawInput);
  const input = validation.input;
  const blockers = [...validation.failures, ...(input.preconditionFailures ?? [])];
  if (!input.exactScalarPrecision) {
    blockers.push(
      `SCALAR_PRECISION_UNAVAILABLE exact BigInt strings are required${
        input.precisionFailures?.length ? `: ${input.precisionFailures.join('; ')}` : ''
      }`
    );
  }

  for (const difference of input.nonLpDifferences) {
    blockers.push(
      `NON_LP_MISMATCH ${difference.user ?? 'global'} Tide ${String(
        difference.epochNumber ?? '-'
      )} ${difference.field}: ${String(difference.baseline)} -> ${String(difference.candidate)}`
    );
  }

  const lpDifferences: EvaluatedLPDifference[] = [];
  for (const row of input.lpDifferences) {
    const rowBlockers: string[] = [];
    const epochNumber = parseExactScalar(row.epochNumber, 'LP epochNumber', rowBlockers);
    const baseline = parseExactScalar(row.baselinePoints, 'LP baselinePoints', rowBlockers);
    const candidate = parseExactScalar(row.candidatePoints, 'LP candidatePoints', rowBlockers);
    const bound =
      row.computedRoundingBound === undefined
        ? undefined
        : parseExactScalar(row.computedRoundingBound, 'LP computedRoundingBound', rowBlockers);
    const user = normalizeUser(row.user);
    const material = row.material ?? 'UserEpochStats';
    let classification: EvaluatedLPDifference['classification'];
    let drift = 0n;
    if (
      rowBlockers.length ||
      epochNumber === undefined ||
      baseline === undefined ||
      candidate === undefined
    ) {
      classification = 'SCALAR_PRECISION_UNAVAILABLE';
      blockers.push(...rowBlockers.map(message => `${message} for ${user}`));
    } else {
      drift = candidate - baseline;
      if (drift === 0n) {
        classification = 'NO_DRIFT';
      } else if (bound !== undefined && bound >= 0n && absBigInt(drift) <= bound) {
        classification = 'WITHIN_COMPUTED_BOUND';
      } else if (row.kind === 'MULTIPLIER_SEMANTIC' && row.note?.trim()) {
        classification = 'MULTIPLIER_SEMANTIC';
      } else {
        classification = 'UNCLASSIFIED_OR_OUT_OF_BOUND';
        blockers.push(
          `LP_DRIFT_BLOCKED ${user} Tide ${epochNumber.toString()} ${row.component}: drift=${drift.toString()} bound=${
            bound?.toString() ?? 'missing'
          }`
        );
      }
      if (bound !== undefined && bound < 0n) {
        blockers.push(
          `LP_BOUND_INVALID ${user} Tide ${epochNumber.toString()} ${row.component}: ${bound.toString()}`
        );
      }
    }
    lpDifferences.push({
      user,
      epochNumber: epochNumber?.toString() ?? String(row.epochNumber),
      component: row.component,
      material,
      baselinePoints: baseline?.toString() ?? String(row.baselinePoints),
      candidatePoints: candidate?.toString() ?? String(row.candidatePoints),
      integerDrift: drift.toString(),
      percentageDrift: baseline === undefined ? 'unavailable' : formatPercentage(drift, baseline),
      computedRoundingBound: bound?.toString(),
      classification,
      note: row.note,
    });
  }

  const rankChanges: ParityEvaluation['rankChanges'] = [];
  for (const ranking of input.closedTideRankings) {
    const epochBlockers: string[] = [];
    const epochNumber = parseExactScalar(
      ranking.epochNumber,
      'closed Tide ranking epochNumber',
      epochBlockers
    );
    const epochLabel = epochNumber?.toString() ?? String(ranking.epochNumber);
    const baseline = exactRankingMaterial(
      ranking.baselineScores,
      `baseline Tide ${epochLabel}`,
      epochBlockers
    );
    const candidate = exactRankingMaterial(
      ranking.candidateScores,
      `candidate Tide ${epochLabel}`,
      epochBlockers
    );
    if (epochBlockers.length) blockers.push(...epochBlockers);
    const users = new Set([...baseline.byUser.keys(), ...candidate.byUser.keys()]);
    const userSetChanged = !sameStringSet(
      new Set(baseline.byUser.keys()),
      new Set(candidate.byUser.keys())
    );
    const rankOrTieChanged = [...users].some(user => {
      const before = baseline.byUser.get(user);
      const after = candidate.byUser.get(user);
      return before?.rank !== after?.rank || before?.tieGroup !== after?.tieGroup;
    });
    const winnerChanged = !sameStringSet(baseline.winners, candidate.winners);
    if (userSetChanged || rankOrTieChanged || winnerChanged) {
      const change = {
        epochNumber: epochLabel,
        winnerChanged,
        baselineOrder: baseline.order,
        candidateOrder: candidate.order,
        baselineCohorts: baseline.cohorts,
        candidateCohorts: candidate.cohorts,
      };
      rankChanges.push(change);
      blockers.push(
        `CLOSED_TIDE_RANK_CHANGE Tide ${change.epochNumber} winnerChanged=${String(
          change.winnerChanged
        )}`
      );
    }
  }

  const finalizationCoverage: EvaluatedFinalizationTide[] = [];
  if (input.finalizationCoverage) {
    const floorBlockers: string[] = [];
    const floor = parseExactScalar(
      input.finalizationCoverage.proposedFloor,
      'proposed final-only floor',
      floorBlockers
    );
    blockers.push(...floorBlockers);
    const parseTides = (values: readonly ExactScalar[] | undefined, label: string) =>
      (values ?? [])
        .map(value => parseExactScalar(value, label, blockers))
        .filter(
          (value): value is bigint => value !== undefined && floor !== undefined && value >= floor
        );
    const declaredRequired = parseTides(
      input.finalizationCoverage.requiredClosedTides,
      'required closed Tide'
    );
    const baselineClosed = parseTides(
      input.finalizationCoverage.baselineClosedTides,
      'baseline closed Tide'
    );
    const candidateClosed = parseTides(
      input.finalizationCoverage.candidateClosedTides,
      'candidate closed Tide'
    );
    if (
      input.finalizationCoverage.baselineClosedTides === undefined ||
      input.finalizationCoverage.candidateClosedTides === undefined
    ) {
      blockers.push('CLOSED_TIDE_SET_UNAVAILABLE baseline and candidate closed sets are required');
    }
    const allDeclared = [...declaredRequired, ...baselineClosed, ...candidateClosed];
    const maximum = allDeclared.reduce<bigint | undefined>(
      (current, value) => (current === undefined || value > current ? value : current),
      undefined
    );
    const requiredTides =
      floor !== undefined && maximum !== undefined ? contiguousRange(floor, maximum, blockers) : [];
    if (floor !== undefined && requiredTides.length === 0) {
      blockers.push(`FINALIZATION_COVERAGE_UNAVAILABLE no closed Tides at or above floor ${floor}`);
    }
    if (!sameScalarSet(requiredTides, declaredRequired)) {
      blockers.push(
        `CLOSED_TIDE_RANGE_MISMATCH required ${describeSetDifference(
          requiredTides,
          declaredRequired
        )}`
      );
    }
    if (!sameScalarSet(requiredTides, baselineClosed)) {
      blockers.push(
        `CLOSED_TIDE_ENDPOINT_MISMATCH baseline ${describeSetDifference(
          requiredTides,
          baselineClosed
        )}`
      );
    }
    if (!sameScalarSet(requiredTides, candidateClosed)) {
      blockers.push(
        `CLOSED_TIDE_ENDPOINT_MISMATCH candidate ${describeSetDifference(
          requiredTides,
          candidateClosed
        )}`
      );
    }
    const rows = new Map<string, FinalizationTideInput>();
    for (const row of input.finalizationCoverage.tides) {
      const epoch = parseExactScalar(row.epochNumber, 'finalization Tide epochNumber', blockers);
      if (epoch !== undefined) rows.set(epoch.toString(), row);
    }
    const manifestRows = new Map<string, ManifestTideInput>();
    for (const row of input.finalizationCoverage.manifestTides ?? []) {
      const epoch = parseExactScalar(row.epochNumber, 'manifest Tide epochNumber', blockers);
      if (epoch !== undefined) manifestRows.set(epoch.toString(), row);
    }
    const coverageEpochs = [...rows.keys()].map(BigInt);
    const manifestEpochs = [...manifestRows.keys()].map(BigInt);
    if (!sameScalarSet(requiredTides, coverageEpochs)) {
      blockers.push(
        `FINALIZATION_COVERAGE_TIDE_MISMATCH ${describeSetDifference(requiredTides, coverageEpochs)}`
      );
    }
    if (!sameScalarSet(requiredTides, manifestEpochs)) {
      blockers.push(
        `MANIFEST_TIDE_MISMATCH ${describeSetDifference(requiredTides, manifestEpochs)}`
      );
    }
    for (const epoch of requiredTides) {
      const row = rows.get(epoch.toString());
      const manifestRow = manifestRows.get(epoch.toString());
      const activePhaseUsers = [...normalizedSet(row?.activePhaseUsers ?? [])].sort();
      const manifestMaterialPresent = manifestRow !== undefined;
      const finalizationMaterialPresent = row?.finalizationUsers !== undefined;
      const manifestUsers = normalizedSet(manifestRow?.users ?? []);
      const finalizationUsers = normalizedSet(row?.finalizationUsers ?? []);
      const missingManifestUsers = activePhaseUsers.filter(user => !manifestUsers.has(user));
      const missingFinalizationUsers = activePhaseUsers.filter(
        user => !finalizationUsers.has(user)
      );
      finalizationCoverage.push({
        epochNumber: epoch.toString(),
        activePhaseUsers,
        missingManifestUsers,
        missingFinalizationUsers,
        manifestMaterialPresent,
        finalizationMaterialPresent,
      });
      if (!row || !manifestMaterialPresent || !finalizationMaterialPresent) {
        blockers.push(
          `FINALIZATION_COVERAGE_UNAVAILABLE Tide ${epoch.toString()} manifest=${String(
            manifestMaterialPresent
          )} finalizations=${String(finalizationMaterialPresent)}`
        );
      }
      if (missingManifestUsers.length) {
        blockers.push(
          `MANIFEST_USERS_MISSING Tide ${epoch.toString()}: ${missingManifestUsers.join(',')}`
        );
      }
      if (missingFinalizationUsers.length) {
        blockers.push(
          `FINALIZATION_USERS_MISSING Tide ${epoch.toString()}: ${missingFinalizationUsers.join(
            ','
          )}`
        );
      }
    }
  }

  return {
    pass: blockers.length === 0,
    verdict: blockers.length === 0 ? 'PASS' : 'BLOCK',
    blockers,
    nonLpDifferences: input.nonLpDifferences,
    lpDifferences,
    rankChanges,
    finalizationCoverage,
  };
}

export function parityExitCode(evaluation: ParityEvaluation): 0 | 1 {
  return evaluation.pass ? 0 : 1;
}

async function gql<T>(endpoint: Endpoint, query: string, runtime: LiveRuntime): Promise<T> {
  const overallRemainingMs = runtime.overallDeadlineAt - Date.now();
  if (overallRemainingMs <= 0) {
    throw new Error('overall live comparison deadline exceeded');
  }
  const requestDeadlineMs = Math.min(runtime.requestTimeoutMs, overallRemainingMs);
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(new Error(`${endpoint.name} GraphQL request timed out`)),
    requestDeadlineMs
  );
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (endpoint.secret) headers['x-hasura-admin-secret'] = endpoint.secret;
  try {
    const response = await fetch(endpoint.url, {
      method: 'POST',
      headers,
      body: JSON.stringify({ query }),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`${endpoint.name} HTTP ${response.status}`);
    const payload = (await response.json()) as { data?: T; errors?: unknown };
    if (payload.errors) {
      throw new Error(`${endpoint.name} GraphQL error: ${JSON.stringify(payload.errors)}`);
    }
    if (!payload.data) throw new Error(`${endpoint.name} returned no data`);
    return payload.data;
  } catch (error) {
    if (controller.signal.aborted) {
      const timeout = new Error(
        `${endpoint.name} GraphQL request timed out after ${requestDeadlineMs}ms`
      );
      (timeout as Error & { cause?: unknown }).cause = error;
      throw timeout;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchAll<T extends { id: string }>(
  endpoint: Endpoint,
  entity: string,
  fields: string,
  runtime: LiveRuntime,
  extraWhere = ''
): Promise<T[]> {
  const pageSize = 1_000;
  const rows: T[] = [];
  let cursor = '';
  let pageCount = 0;
  for (;;) {
    if (pageCount >= runtime.maxPages) {
      throw new Error(`${endpoint.name} ${entity} page ceiling ${runtime.maxPages} exceeded`);
    }
    const filters = [`id: { _gt: ${JSON.stringify(cursor)} }`];
    if (extraWhere) filters.push(extraWhere);
    const query = `query { ${entity}(where: { ${filters.join(
      ', '
    )} }, order_by: { id: asc }, limit: ${pageSize}) { ${fields} } }`;
    const data = await gql<Record<string, unknown>>(endpoint, query, runtime);
    const page = data[entity];
    pageCount += 1;
    if (!Array.isArray(page)) {
      throw new Error(`${endpoint.name} ${entity} pagination page must be an array`);
    }
    if (page.length > pageSize) {
      throw new Error(
        `${endpoint.name} ${entity} pagination page size ${page.length} exceeds limit ${pageSize}`
      );
    }
    if (rows.length + page.length > runtime.maxRows) {
      throw new Error(`${endpoint.name} ${entity} row ceiling ${runtime.maxRows} exceeded`);
    }
    let previousID = cursor;
    for (const [index, row] of page.entries()) {
      if (!isRecord(row) || typeof row.id !== 'string' || row.id.length === 0) {
        throw new Error(
          `${endpoint.name} ${entity} pagination page ${pageCount} row ${index} has a missing or non-string id`
        );
      }
      if (row.id <= previousID) {
        throw new Error(
          `${endpoint.name} ${entity} pagination IDs must be strictly increasing beyond cursor ${JSON.stringify(
            cursor
          )}`
        );
      }
      previousID = row.id;
    }
    rows.push(...page);
    if (page.length < pageSize) return rows;
    const nextCursor = (page[page.length - 1] as { id: string }).id;
    if (nextCursor <= cursor) {
      throw new Error(`${endpoint.name} ${entity} pagination cursor did not advance`);
    }
    cursor = nextCursor;
  }
}

export type UserPointsParityRow = {
  id: string;
  user_id: string;
  lifetimeDepositPoints: ExactScalar;
  lifetimeBorrowPoints: ExactScalar;
  lifetimeDailySupplyPoints: ExactScalar;
  lifetimeDailyBorrowPoints: ExactScalar;
  lifetimeDailyRepayPoints: ExactScalar;
  lifetimeDailyWithdrawPoints: ExactScalar;
  lifetimeDailyVPPoints: ExactScalar;
  lifetimeTotalPoints: ExactScalar;
  epochsParticipated: ExactScalar[];
  lifetimeEpochsIncluded: ExactScalar[];
  lastUpdatedAt: number;
};

export type UserLeaderboardStateParityRow = {
  id: string;
  user_id: string;
  nftCount: ExactScalar;
  nftMultiplier: ExactScalar;
  specialEditionCount: ExactScalar;
  specialEditionMultiplier: ExactScalar;
  votingPower: ExactScalar;
  vpTierIndex: ExactScalar;
  vpMultiplier: ExactScalar;
  combinedMultiplier: ExactScalar;
  totalEpochsParticipated: ExactScalar;
  lifetimePoints: ExactScalar;
  currentEpochId: string | null;
  currentEpochRank: ExactScalar;
  lastUpdate: number;
};

export type UserEpochParityRow = {
  id: string;
  user_id: string;
  epochNumber: ExactScalar;
  depositPoints: ExactScalar;
  borrowPoints: ExactScalar;
  lpPoints: ExactScalar;
  dailySupplyPoints: ExactScalar;
  dailyBorrowPoints: ExactScalar;
  dailyRepayPoints: ExactScalar;
  dailyWithdrawPoints: ExactScalar;
  dailyVPPoints: ExactScalar;
  dailyLPPoints: ExactScalar;
  manualAwardPoints: ExactScalar;
  depositMultiplierBps: ExactScalar;
  borrowMultiplierBps: ExactScalar;
  vpMultiplierBps: ExactScalar;
  lpMultiplierBps: ExactScalar;
  depositPointsWithMultiplier: ExactScalar;
  borrowPointsWithMultiplier: ExactScalar;
  vpPointsWithMultiplier: ExactScalar;
  lpPointsWithMultiplier: ExactScalar;
  lastSupplyPointsDay: number;
  lastBorrowPointsDay: number;
  lastRepayPointsDay: number;
  lastWithdrawPointsDay: number;
  lastVPPointsDay: number;
  lastVPAccrualTimestamp: number;
  totalPoints: ExactScalar;
  totalPointsWithMultiplier: ExactScalar;
  totalMultiplierBps: ExactScalar;
  lastAppliedMultiplierBps: ExactScalar;
  testnetBonusBps: ExactScalar;
  rank: number | null;
  firstSeenAt: number;
  lastUpdatedAt: number;
};

export type LeaderboardParitySnapshot = {
  userPoints: readonly UserPointsParityRow[];
  userLeaderboardStates: readonly UserLeaderboardStateParityRow[];
  userEpochStats: readonly UserEpochParityRow[];
};

type EpochRow = {
  id: string;
  epochNumber: ExactScalar;
  startBlock: ExactScalar;
  startTime: number;
  endBlock: ExactScalar;
  endTime: number | null;
  isActive: boolean;
};

type KeeperRawRow = {
  id: string;
  user_id: string;
  epochNumber: ExactScalar;
  isGap: boolean;
};

type FinalizationRow = { id: string; user_id: string; epochNumber: ExactScalar };

export type LPClassificationInput = {
  user: string;
  epochNumber: ExactScalar;
  component: LPComponent;
  material?: ParityEntity;
  computedRoundingBound?: ExactScalar;
  kind?: 'MULTIPLIER_SEMANTIC';
  note?: string;
};

type ManifestFile = {
  tides: Array<{ epochNumber: ExactScalar; users: string[] }>;
};

function readJsonFile(path: string): unknown {
  return JSON.parse(readFileSync(path, 'utf8')) as unknown;
}

function exactScalar(value: ExactScalar): boolean {
  return typeof value === 'string' && /^-?[0-9]+$/.test(value);
}

function scalarKey(value: ExactScalar): string {
  return exactScalar(value) ? BigInt(value as string).toString() : String(value);
}

function scalarDifferent(left: ExactScalar, right: ExactScalar): boolean {
  if (!exactScalar(left) || !exactScalar(right)) return String(left) !== String(right);
  return BigInt(left as string) !== BigInt(right as string);
}

function classificationKey(
  user: string,
  epochNumber: ExactScalar,
  component: LPComponent,
  material: ParityEntity
): string {
  return `${material}:${normalizeUser(user)}:${String(epochNumber)}:${component}`;
}

export function validateLPClassificationFile(raw: unknown): {
  rows: LPClassificationInput[];
  failures: string[];
} {
  const rows: LPClassificationInput[] = [];
  const failures: string[] = [];
  if (!isRecord(raw) || !Array.isArray(raw.rows)) {
    inputFailure(failures, 'classification.rows', 'expected array');
    return { rows, failures };
  }
  const seen = new Set<string>();
  for (const [index, value] of raw.rows.entries()) {
    const path = `classification.rows[${index}]`;
    if (!isRecord(value)) {
      inputFailure(failures, path, 'expected object');
      continue;
    }
    if (!isAddress(value.user)) inputFailure(failures, `${path}.user`, 'expected address');
    const material =
      value.material === undefined
        ? 'UserEpochStats'
        : typeof value.material === 'string' &&
            PARITY_ENTITY_SET.has(value.material as ParityEntity)
          ? (value.material as ParityEntity)
          : undefined;
    if (!material) inputFailure(failures, `${path}.material`, 'unknown entity');
    const component =
      typeof value.component === 'string' && LP_COMPONENT_SET.has(value.component as LPComponent)
        ? (value.component as LPComponent)
        : undefined;
    if (!component) inputFailure(failures, `${path}.component`, 'unknown component');
    if (component && material && !componentBelongsToMaterial(component, material)) {
      inputFailure(failures, `${path}.component`, `not valid for ${material}`);
    }
    const epochNumber = validatedExactScalar(value.epochNumber, `${path}.epochNumber`, failures);
    const computedRoundingBound =
      value.computedRoundingBound === undefined
        ? undefined
        : validatedExactScalar(
            value.computedRoundingBound,
            `${path}.computedRoundingBound`,
            failures
          );
    const kind =
      value.kind === undefined || value.kind === 'MULTIPLIER_SEMANTIC' ? value.kind : undefined;
    if (value.kind !== undefined && kind === undefined) {
      inputFailure(failures, `${path}.kind`, 'unknown enum value');
    }
    if (value.note !== undefined && typeof value.note !== 'string') {
      inputFailure(failures, `${path}.note`, 'expected string');
    }
    if (kind === 'MULTIPLIER_SEMANTIC' && !(typeof value.note === 'string' && value.note.trim())) {
      inputFailure(failures, `${path}.note`, 'required for MULTIPLIER_SEMANTIC');
    }
    if (isAddress(value.user) && material && component && epochNumber) {
      const key = classificationKey(value.user, epochNumber, component, material);
      if (seen.has(key)) inputFailure(failures, path, `duplicate classification key ${key}`);
      seen.add(key);
    }
    if (
      isAddress(value.user) &&
      material &&
      component &&
      componentBelongsToMaterial(component, material) &&
      epochNumber &&
      (value.computedRoundingBound === undefined || computedRoundingBound !== undefined) &&
      (value.kind === undefined || kind !== undefined) &&
      (value.note === undefined || typeof value.note === 'string')
    ) {
      rows.push({
        user: value.user,
        epochNumber,
        component,
        material,
        computedRoundingBound,
        kind,
        note: value.note as string | undefined,
      });
    }
  }
  return { rows, failures };
}

export function validateArchivedManifestFile(raw: unknown): {
  manifest?: ManifestFile;
  failures: string[];
} {
  const failures: string[] = [];
  if (!isRecord(raw) || !Array.isArray(raw.tides)) {
    inputFailure(failures, 'manifest.tides', 'expected array');
    return { failures };
  }
  const tides: ManifestFile['tides'] = [];
  const seen = new Set<string>();
  for (const [index, value] of raw.tides.entries()) {
    const path = `manifest.tides[${index}]`;
    if (!isRecord(value)) {
      inputFailure(failures, path, 'expected object');
      continue;
    }
    const epochNumber = validatedExactScalar(value.epochNumber, `${path}.epochNumber`, failures);
    if (epochNumber && seen.has(epochNumber)) {
      inputFailure(failures, path, `duplicate Tide ${epochNumber}`);
    }
    if (epochNumber) seen.add(epochNumber);
    const users = validatedStringArray(value.users, `${path}.users`, failures, {
      addresses: true,
    });
    if (epochNumber) tides.push({ epochNumber, users });
  }
  return { manifest: { tides }, failures };
}

export type UserEpochParityMaterial = Pick<
  ParityEvaluationInput,
  'preconditionFailures' | 'nonLpDifferences' | 'lpDifferences' | 'closedTideRankings'
> & { precisionFailures: string[] };

type SnapshotRow = Record<string, unknown>;

function snapshotIdentity(
  entity: ParityEntity,
  row: SnapshotRow,
  label: string,
  preconditionFailures: string[],
  precisionFailures: string[]
): string | undefined {
  if (!isAddress(row.user_id)) {
    preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label}.user_id expected address`);
    return undefined;
  }
  const user = normalizeUser(row.user_id);
  let identity = user;
  if (entity === 'UserEpochStats') {
    if (!isNonnegativeExactInteger(row.epochNumber)) {
      precisionFailures.push(`${label}.epochNumber`);
      return undefined;
    }
    identity = `${user}:${BigInt(row.epochNumber).toString()}`;
  }
  if (typeof row.id !== 'string' || row.id.toLowerCase() !== identity) {
    preconditionFailures.push(
      `SNAPSHOT_IDENTITY_INVALID ${label}.id expected=${identity} actual=${String(row.id)}`
    );
  }
  return identity;
}

function canonicalSnapshotValue(
  value: unknown,
  rule: ParityFieldRule,
  label: string,
  preconditionFailures: string[],
  precisionFailures: string[]
): string | undefined {
  if (rule.valueType === 'BIGINT' || rule.valueType === 'NULLABLE_BIGINT') {
    if (value === null && rule.valueType === 'NULLABLE_BIGINT') return 'null';
    if (!isExactInteger(value)) {
      precisionFailures.push(label);
      return undefined;
    }
    return BigInt(value).toString();
  }
  if (rule.valueType === 'INT' || rule.valueType === 'NULLABLE_INT') {
    if (value === null && rule.valueType === 'NULLABLE_INT') return 'null';
    if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
      preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} expected safe integer`);
      return undefined;
    }
    return String(value);
  }
  if (rule.valueType === 'ADDRESS') {
    if (!isAddress(value)) {
      preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} expected address`);
      return undefined;
    }
    return normalizeUser(value);
  }
  if (rule.valueType === 'BIGINT_SET') {
    if (!Array.isArray(value)) {
      preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} expected array`);
      return undefined;
    }
    const values: bigint[] = [];
    const seen = new Set<string>();
    for (const [index, entry] of value.entries()) {
      if (!isNonnegativeExactInteger(entry)) {
        precisionFailures.push(`${label}[${index}]`);
        continue;
      }
      const canonical = BigInt(entry).toString();
      if (seen.has(canonical)) {
        preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} duplicate ${canonical}`);
      }
      seen.add(canonical);
      values.push(BigInt(canonical));
    }
    values.sort((left, right) => (left === right ? 0 : left < right ? -1 : 1));
    return JSON.stringify(values.map(String));
  }
  if (value === null && rule.valueType === 'NULLABLE_STRING') return 'null';
  if (typeof value !== 'string' || !value.trim()) {
    preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} expected string`);
    return undefined;
  }
  return value;
}

function indexSnapshotRows(
  entity: ParityEntity,
  rows: readonly unknown[],
  endpoint: 'baseline' | 'candidate',
  preconditionFailures: string[],
  precisionFailures: string[]
): Map<string, SnapshotRow> {
  const indexed = new Map<string, SnapshotRow>();
  for (const [index, value] of rows.entries()) {
    const label = `${endpoint}.${entity}[${index}]`;
    if (!isRecord(value)) {
      preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID ${label} expected object`);
      continue;
    }
    const identity = snapshotIdentity(
      entity,
      value,
      label,
      preconditionFailures,
      precisionFailures
    );
    if (!identity) continue;
    if (indexed.has(identity)) {
      preconditionFailures.push(`DUPLICATE_ENDPOINT_IDENTITY ${label} ${identity}`);
      continue;
    }
    indexed.set(identity, value);
    for (const rule of LEADERBOARD_PARITY_FIELD_MANIFEST[entity]) {
      if (rule.category === 'EXCLUDED') continue;
      canonicalSnapshotValue(
        value[rule.field],
        rule,
        `${label}.${rule.field}`,
        preconditionFailures,
        precisionFailures
      );
    }
  }
  return indexed;
}

function groupScoresByEpoch(rows: readonly UserEpochParityRow[]) {
  const grouped = new Map<string, ClosedTideScoreInput[]>();
  for (const row of rows) {
    const epoch = scalarKey(row.epochNumber);
    if (epoch === '0') continue;
    const scores = grouped.get(epoch) ?? [];
    scores.push({
      user: row.user_id,
      totalPoints: row.totalPoints,
      totalPointsWithMultiplier: row.totalPointsWithMultiplier,
    });
    grouped.set(epoch, scores);
  }
  return grouped;
}

export type LeaderboardParityMaterial = UserEpochParityMaterial;

export function buildLeaderboardParityMaterial(
  baseline: LeaderboardParitySnapshot,
  candidate: LeaderboardParitySnapshot,
  closedEpochs: readonly ExactScalar[],
  classificationRows: readonly LPClassificationInput[] = []
): LeaderboardParityMaterial {
  const preconditionFailures: string[] = [];
  const precisionFailures: string[] = [];
  const nonLpDifferences: NonLPDifference[] = [];
  const lpDifferences: LPDifferenceInput[] = [];
  const classificationValidation = validateLPClassificationFile({ rows: classificationRows });
  preconditionFailures.push(...classificationValidation.failures);
  const classifications = new Map(
    classificationValidation.rows.map(row => {
      const material = row.material ?? 'UserEpochStats';
      return [classificationKey(row.user, row.epochNumber, row.component, material), row];
    })
  );

  const snapshots: Array<{
    entity: ParityEntity;
    baselineRows: readonly unknown[];
    candidateRows: readonly unknown[];
  }> = [
    {
      entity: 'UserPoints',
      baselineRows: baseline.userPoints,
      candidateRows: candidate.userPoints,
    },
    {
      entity: 'UserLeaderboardState',
      baselineRows: baseline.userLeaderboardStates,
      candidateRows: candidate.userLeaderboardStates,
    },
    {
      entity: 'UserEpochStats',
      baselineRows: baseline.userEpochStats,
      candidateRows: candidate.userEpochStats,
    },
  ];

  for (const snapshot of snapshots) {
    const baselineBy = indexSnapshotRows(
      snapshot.entity,
      snapshot.baselineRows,
      'baseline',
      preconditionFailures,
      precisionFailures
    );
    const candidateBy = indexSnapshotRows(
      snapshot.entity,
      snapshot.candidateRows,
      'candidate',
      preconditionFailures,
      precisionFailures
    );
    const identities = [...new Set([...baselineBy.keys(), ...candidateBy.keys()])].sort();
    for (const identity of identities) {
      const baselineRow = baselineBy.get(identity);
      const candidateRow = candidateBy.get(identity);
      const user = identity.split(':')[0];
      const epochNumber = snapshot.entity === 'UserEpochStats' ? identity.split(':')[1] : '0';
      if (!baselineRow || !candidateRow) {
        nonLpDifferences.push({
          user,
          epochNumber: snapshot.entity === 'UserEpochStats' ? epochNumber : undefined,
          field: `${snapshot.entity} row`,
          baseline: baselineRow ? '1' : null,
          candidate: candidateRow ? '1' : null,
        });
        continue;
      }
      for (const rule of LEADERBOARD_PARITY_FIELD_MANIFEST[snapshot.entity]) {
        if (rule.category === 'IDENTITY' || rule.category === 'EXCLUDED') continue;
        const baselineValue = canonicalSnapshotValue(
          baselineRow[rule.field],
          rule,
          `baseline.${snapshot.entity}.${identity}.${rule.field}`,
          preconditionFailures,
          precisionFailures
        );
        const candidateValue = canonicalSnapshotValue(
          candidateRow[rule.field],
          rule,
          `candidate.${snapshot.entity}.${identity}.${rule.field}`,
          preconditionFailures,
          precisionFailures
        );
        if (
          baselineValue === undefined ||
          candidateValue === undefined ||
          baselineValue === candidateValue
        ) {
          continue;
        }
        if (rule.category === 'EXACT') {
          nonLpDifferences.push({
            user,
            epochNumber: snapshot.entity === 'UserEpochStats' ? epochNumber : undefined,
            field: `${snapshot.entity}.${rule.field}`,
            baseline: baselineValue,
            candidate: candidateValue,
          });
          continue;
        }
        if (rule.comparison === 'CONSISTENCY') {
          preconditionFailures.push(
            `LP_MATERIAL_MISMATCH ${user} Tide ${epochNumber} ${snapshot.entity}.${rule.field}: ${baselineValue} -> ${candidateValue}`
          );
          continue;
        }
        const component = rule.field as LPComponent;
        const classification = classifications.get(
          classificationKey(user, epochNumber, component, snapshot.entity)
        );
        lpDifferences.push({
          user,
          epochNumber,
          material: snapshot.entity,
          component,
          baselinePoints: baselineValue,
          candidatePoints: candidateValue,
          computedRoundingBound: classification?.computedRoundingBound,
          kind: classification?.kind,
          note: classification?.note,
        });
      }
    }
  }

  const verifyLifetimeConsistency = (
    snapshot: LeaderboardParitySnapshot,
    endpoint: 'baseline' | 'candidate'
  ) => {
    const pointsByUser = new Map(snapshot.userPoints.map(row => [normalizeUser(row.user_id), row]));
    const stateByUser = new Map(
      snapshot.userLeaderboardStates.map(row => [normalizeUser(row.user_id), row])
    );
    for (const user of new Set([...pointsByUser.keys(), ...stateByUser.keys()])) {
      const points = pointsByUser.get(user);
      const state = stateByUser.get(user);
      if (
        points &&
        state &&
        exactScalar(points.lifetimeTotalPoints) &&
        exactScalar(state.lifetimePoints) &&
        scalarDifferent(points.lifetimeTotalPoints, state.lifetimePoints)
      ) {
        preconditionFailures.push(
          `LP_LIFETIME_CONSISTENCY_MISMATCH ${endpoint} ${user}: UserPoints.lifetimeTotalPoints=${String(
            points.lifetimeTotalPoints
          )} UserLeaderboardState.lifetimePoints=${String(state.lifetimePoints)}`
        );
      }
    }
  };
  verifyLifetimeConsistency(baseline, 'baseline');
  verifyLifetimeConsistency(candidate, 'candidate');

  const baselineScores = groupScoresByEpoch(baseline.userEpochStats);
  const candidateScores = groupScoresByEpoch(candidate.userEpochStats);
  const uniqueClosedEpochs = [...new Set(closedEpochs.map(scalarKey))]
    .filter(epoch => epoch !== '0')
    .sort((left, right) => left.localeCompare(right, undefined, { numeric: true }));
  const closedTideRankings = uniqueClosedEpochs.map(epochNumber => ({
    epochNumber,
    baselineScores: baselineScores.get(epochNumber) ?? [],
    candidateScores: candidateScores.get(epochNumber) ?? [],
  }));

  return {
    preconditionFailures,
    precisionFailures,
    nonLpDifferences,
    lpDifferences,
    closedTideRankings,
  };
}

export function buildUserEpochParityMaterial(
  baselineRows: readonly UserEpochParityRow[],
  candidateRows: readonly UserEpochParityRow[],
  closedEpochs: readonly ExactScalar[],
  classificationRows: readonly LPClassificationInput[] = []
): UserEpochParityMaterial {
  return buildLeaderboardParityMaterial(
    {
      userPoints: [],
      userLeaderboardStates: [],
      userEpochStats: baselineRows,
    },
    {
      userPoints: [],
      userLeaderboardStates: [],
      userEpochStats: candidateRows,
    },
    closedEpochs,
    classificationRows
  );
}

async function maxLastUpdate(endpoint: Endpoint, runtime: LiveRuntime): Promise<number> {
  const query = `query { UserLeaderboardState(order_by: { lastUpdate: desc }, limit: 1) { lastUpdate } }`;
  const data = await gql<Record<string, unknown>>(endpoint, query, runtime);
  const rows = data.UserLeaderboardState;
  if (!Array.isArray(rows) || rows.length > 1) {
    throw new Error(
      `${endpoint.name} activity watermark payload must be an array of at most one row`
    );
  }
  if (rows.length === 0) return 0;
  const value = isRecord(rows[0]) ? rows[0].lastUpdate : undefined;
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new Error(`${endpoint.name} activity watermark must be a nonnegative safe integer`);
  }
  return Number(value);
}

async function chainMetadata(endpoint: Endpoint, runtime: LiveRuntime): Promise<unknown> {
  const query = `query { chain_metadata(where: { chain_id: { _eq: 143 } }, limit: 1) { chain_id latest_processed_block } }`;
  const data = await gql<Record<string, unknown>>(endpoint, query, runtime);
  return data.chain_metadata;
}

async function buildLiveInput(runtime: LiveRuntime): Promise<ParityEvaluationInput> {
  const fieldsFor = (entity: ParityEntity) =>
    LEADERBOARD_PARITY_FIELD_MANIFEST[entity].map(rule => rule.field).join(' ');
  const [
    baselineChainMetadata,
    candidateChainMetadata,
    baselineTip,
    candidateTip,
    baselinePoints,
    candidatePoints,
    baselineStates,
    candidateStates,
    baselineStats,
    candidateStats,
    baselineEpochs,
    candidateEpochs,
  ] = await Promise.all([
    chainMetadata(runtime.baseline, runtime),
    chainMetadata(runtime.candidate, runtime),
    maxLastUpdate(runtime.baseline, runtime),
    maxLastUpdate(runtime.candidate, runtime),
    fetchAll<UserPointsParityRow>(runtime.baseline, 'UserPoints', fieldsFor('UserPoints'), runtime),
    fetchAll<UserPointsParityRow>(
      runtime.candidate,
      'UserPoints',
      fieldsFor('UserPoints'),
      runtime
    ),
    fetchAll<UserLeaderboardStateParityRow>(
      runtime.baseline,
      'UserLeaderboardState',
      fieldsFor('UserLeaderboardState'),
      runtime
    ),
    fetchAll<UserLeaderboardStateParityRow>(
      runtime.candidate,
      'UserLeaderboardState',
      fieldsFor('UserLeaderboardState'),
      runtime
    ),
    fetchAll<UserEpochParityRow>(
      runtime.baseline,
      'UserEpochStats',
      fieldsFor('UserEpochStats'),
      runtime
    ),
    fetchAll<UserEpochParityRow>(
      runtime.candidate,
      'UserEpochStats',
      fieldsFor('UserEpochStats'),
      runtime
    ),
    fetchAll<EpochRow>(
      runtime.baseline,
      'LeaderboardEpoch',
      'id epochNumber startBlock startTime endBlock endTime isActive',
      runtime
    ),
    fetchAll<EpochRow>(
      runtime.candidate,
      'LeaderboardEpoch',
      'id epochNumber startBlock startTime endBlock endTime isActive',
      runtime
    ),
  ]);

  const preconditionFailures: string[] = [];
  const precisionFailures: string[] = [];
  const validateChainMetadata = (value: unknown, endpoint: string): void => {
    if (!Array.isArray(value) || value.length !== 1 || !isRecord(value[0])) {
      preconditionFailures.push(
        `CHAIN_METADATA_INVALID ${endpoint}: expected exactly one Monad chain 143 row`
      );
      return;
    }
    const row = value[0];
    const chainID = row.chain_id === 143 || row.chain_id === '143' ? '143' : String(row.chain_id);
    const latestProcessedBlock =
      typeof row.latest_processed_block === 'string' &&
      /^[1-9][0-9]*$/.test(row.latest_processed_block)
        ? BigInt(row.latest_processed_block).toString()
        : typeof row.latest_processed_block === 'number' &&
            Number.isSafeInteger(row.latest_processed_block) &&
            row.latest_processed_block > 0
          ? String(row.latest_processed_block)
          : undefined;
    if (chainID !== '143' || latestProcessedBlock === undefined) {
      preconditionFailures.push(
        `CHAIN_METADATA_INVALID ${endpoint}: chain=${chainID} latest=${String(
          row.latest_processed_block
        )}`
      );
      return;
    }
    if (latestProcessedBlock !== runtime.expectedTargetBlock) {
      preconditionFailures.push(
        `TARGET_BLOCK_MISMATCH ${endpoint}: expected=${runtime.expectedTargetBlock} actual=${latestProcessedBlock}`
      );
    }
  };
  validateChainMetadata(baselineChainMetadata, 'baseline');
  validateChainMetadata(candidateChainMetadata, 'candidate');

  for (const [endpoint, collections] of [
    [
      'baseline',
      [
        ['UserPoints', baselinePoints],
        ['UserLeaderboardState', baselineStates],
        ['UserEpochStats', baselineStats],
        ['LeaderboardEpoch', baselineEpochs],
      ],
    ],
    [
      'candidate',
      [
        ['UserPoints', candidatePoints],
        ['UserLeaderboardState', candidateStates],
        ['UserEpochStats', candidateStats],
        ['LeaderboardEpoch', candidateEpochs],
      ],
    ],
  ] as const) {
    for (const [entity, rows] of collections) {
      if (rows.length === 0) {
        preconditionFailures.push(`SNAPSHOT_MATERIAL_EMPTY ${endpoint}.${entity}`);
      }
    }
  }
  if (Math.abs(candidateTip - baselineTip) > 600) {
    preconditionFailures.push(
      `ACTIVITY_WATERMARK_MISMATCH baseline=${baselineTip} candidate=${candidateTip}`
    );
  }
  const classificationPath = process.env.PARITY_LP_CLASSIFICATIONS_PATH?.trim();
  const classificationValidation = classificationPath
    ? validateLPClassificationFile(readJsonFile(classificationPath))
    : { rows: [], failures: [] };
  preconditionFailures.push(...classificationValidation.failures);

  const closedEpochSet = (rows: readonly EpochRow[], endpoint: string): Set<string> => {
    const result = new Set<string>();
    for (const row of rows) {
      const label = `${endpoint}.LeaderboardEpoch.${String(row.id)}`;
      const epochValid =
        typeof row.epochNumber === 'string' && /^[1-9][0-9]*$/.test(row.epochNumber);
      const canonicalEpoch = epochValid ? BigInt(row.epochNumber as string).toString() : undefined;
      if (!canonicalEpoch || row.id !== canonicalEpoch) {
        preconditionFailures.push(
          `EPOCH_MATERIAL_INVALID ${label}.id/epochNumber expected canonical positive identity`
        );
        continue;
      }
      if (
        typeof row.startBlock !== 'string' ||
        !/^[1-9][0-9]*$/.test(row.startBlock) ||
        !Number.isSafeInteger(row.startTime) ||
        row.startTime <= 0
      ) {
        preconditionFailures.push(
          `EPOCH_MATERIAL_INVALID ${label}.startBlock/startTime expected positive exact values`
        );
        continue;
      }
      if (typeof row.isActive !== 'boolean') {
        preconditionFailures.push(`EPOCH_MATERIAL_INVALID ${label}.isActive expected boolean`);
        continue;
      }
      if (row.isActive) {
        if (row.endBlock !== null || row.endTime !== null) {
          preconditionFailures.push(
            `EPOCH_MATERIAL_INVALID ${label} active epoch must have null endBlock/endTime`
          );
        }
        continue;
      }
      if (
        typeof row.endBlock !== 'string' ||
        !/^[1-9][0-9]*$/.test(row.endBlock) ||
        !Number.isSafeInteger(row.endTime) ||
        row.endTime === null ||
        row.endTime <= 0
      ) {
        preconditionFailures.push(
          `EPOCH_MATERIAL_INVALID ${label}.endBlock/endTime expected positive exact closed values`
        );
        continue;
      }
      if (BigInt(row.endBlock) < BigInt(row.startBlock) || row.endTime < row.startTime) {
        preconditionFailures.push(`EPOCH_MATERIAL_INVALID ${label} closure precedes epoch start`);
        continue;
      }
      if (result.has(canonicalEpoch)) {
        preconditionFailures.push(
          `DUPLICATE_ENDPOINT_IDENTITY ${endpoint}.LeaderboardEpoch ${canonicalEpoch}`
        );
      }
      result.add(canonicalEpoch);
    }
    return result;
  };

  const baselineClosed = closedEpochSet(baselineEpochs, 'baseline');
  const candidateClosed = closedEpochSet(candidateEpochs, 'candidate');
  const baselineOnly = [...baselineClosed].filter(epoch => !candidateClosed.has(epoch)).sort();
  const candidateOnly = [...candidateClosed].filter(epoch => !baselineClosed.has(epoch)).sort();
  if (baselineOnly.length || candidateOnly.length) {
    preconditionFailures.push(
      `CLOSED_TIDE_MATERIAL_MISMATCH baselineOnly=[${baselineOnly.join(
        ','
      )}] candidateOnly=[${candidateOnly.join(',')}]`
    );
  }
  const closedEpochs = [...new Set([...baselineClosed, ...candidateClosed])];
  const material = buildLeaderboardParityMaterial(
    {
      userPoints: baselinePoints,
      userLeaderboardStates: baselineStates,
      userEpochStats: baselineStats,
    },
    {
      userPoints: candidatePoints,
      userLeaderboardStates: candidateStates,
      userEpochStats: candidateStats,
    },
    closedEpochs,
    classificationValidation.rows
  );
  preconditionFailures.push(...(material.preconditionFailures ?? []));
  precisionFailures.push(...material.precisionFailures);

  const floorRaw =
    process.env.PARITY_PROPOSED_FINAL_ONLY_FLOOR?.trim() ||
    process.env.ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH?.trim();
  let finalizationCoverage: ParityEvaluationInput['finalizationCoverage'];
  if (floorRaw) {
    const floor = /^[0-9]+$/.test(floorRaw) ? BigInt(floorRaw) : undefined;
    const [rawRows, finalizationRows] = await Promise.all([
      fetchAll<KeeperRawRow>(
        runtime.candidate,
        'LeaderboardKeeperUserSettled',
        'id user_id epochNumber isGap',
        runtime,
        'isGap: { _eq: false }'
      ),
      fetchAll<FinalizationRow>(
        runtime.candidate,
        'UserEpochFinalization',
        'id user_id epochNumber',
        runtime
      ),
    ]);
    const manifestPath = process.env.PARITY_ARCHIVED_MANIFEST_PATH?.trim();
    const manifestValidation = manifestPath
      ? validateArchivedManifestFile(readJsonFile(manifestPath))
      : { manifest: undefined, failures: [] };
    preconditionFailures.push(...manifestValidation.failures);
    for (const [index, row] of rawRows.entries()) {
      if (!isAddress(row.user_id)) {
        preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID KeeperRaw[${index}].user_id`);
      }
      if (!isNonnegativeExactInteger(row.epochNumber)) {
        precisionFailures.push(`KeeperRaw[${index}].epochNumber`);
      }
      if (typeof row.isGap !== 'boolean') {
        preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID KeeperRaw[${index}].isGap`);
      }
    }
    const validFinalizationRows: FinalizationRow[] = [];
    for (const [index, row] of finalizationRows.entries()) {
      if (!isAddress(row.user_id)) {
        preconditionFailures.push(`SNAPSHOT_MATERIAL_INVALID Finalization[${index}].user_id`);
      }
      if (!isNonnegativeExactInteger(row.epochNumber)) {
        precisionFailures.push(`Finalization[${index}].epochNumber`);
      }
      if (isAddress(row.user_id) && isNonnegativeExactInteger(row.epochNumber)) {
        const expectedID = `${normalizeUser(row.user_id)}:${BigInt(row.epochNumber).toString()}`;
        if (typeof row.id !== 'string' || row.id.toLowerCase() !== expectedID) {
          preconditionFailures.push(
            `FINALIZATION_IDENTITY_INVALID Finalization[${index}] expected=${expectedID} actual=${String(
              row.id
            )}`
          );
        } else {
          validFinalizationRows.push(row);
        }
      }
    }
    const manifestTidesAtOrAboveFloor = manifestValidation.manifest?.tides.filter(
      row =>
        floor !== undefined &&
        isNonnegativeExactInteger(row.epochNumber) &&
        BigInt(row.epochNumber) >= floor
    );
    const evidenceAtOrAboveFloor = [
      ...baselineClosed,
      ...candidateClosed,
      ...rawRows.flatMap(row =>
        row.isGap === false && isNonnegativeExactInteger(row.epochNumber)
          ? [BigInt(row.epochNumber).toString()]
          : []
      ),
      ...validFinalizationRows.flatMap(row =>
        isNonnegativeExactInteger(row.epochNumber) ? [BigInt(row.epochNumber).toString()] : []
      ),
      ...(manifestTidesAtOrAboveFloor ?? []).map(row =>
        BigInt(row.epochNumber as string).toString()
      ),
    ]
      .map(BigInt)
      .filter(epoch => floor !== undefined && epoch >= floor);
    const maximum = evidenceAtOrAboveFloor.reduce<bigint | undefined>(
      (current, epoch) => (current === undefined || epoch > current ? epoch : current),
      undefined
    );
    const requiredClosedTides =
      floor !== undefined && maximum !== undefined
        ? contiguousRange(floor, maximum, preconditionFailures).map(String)
        : [];
    finalizationCoverage = {
      proposedFloor: floorRaw,
      baselineClosedTides: [...baselineClosed],
      candidateClosedTides: [...candidateClosed],
      requiredClosedTides,
      tides: requiredClosedTides.map(epochNumber => {
        const epoch = scalarKey(epochNumber);
        return {
          epochNumber,
          activePhaseUsers: rawRows
            .filter(row => scalarKey(row.epochNumber) === epoch && !row.isGap)
            .map(row => row.user_id),
          finalizationUsers: validFinalizationRows
            .filter(row => scalarKey(row.epochNumber) === epoch)
            .map(row => row.user_id),
        };
      }),
      manifestTides: manifestTidesAtOrAboveFloor,
    };
  }

  return {
    exactScalarPrecision: precisionFailures.length === 0,
    precisionFailures,
    preconditionFailures,
    nonLpDifferences: material.nonLpDifferences,
    lpDifferences: material.lpDifferences,
    closedTideRankings: material.closedTideRankings,
    finalizationCoverage,
  };
}

function printEvaluation(evaluation: ParityEvaluation): void {
  console.log(`Rollout parity verdict: ${evaluation.verdict}`);
  console.log(`Exact non-LP differences: ${evaluation.nonLpDifferences.length}`);
  console.log(`Per-user/per-Tide LP differences: ${evaluation.lpDifferences.length}`);
  for (const row of evaluation.lpDifferences) {
    console.log(
      `  ${row.user} Tide ${row.epochNumber} ${row.component}: drift=${row.integerDrift} (${row.percentageDrift}) bound=${
        row.computedRoundingBound ?? 'missing'
      } classification=${row.classification}`
    );
  }
  console.log(`Closed-Tide rank changes: ${evaluation.rankChanges.length}`);
  for (const row of evaluation.rankChanges) {
    console.log(
      `  Tide ${row.epochNumber} winnerChanged=${String(row.winnerChanged)} baseline=[${row.baselineOrder.join(
        ','
      )}] candidate=[${row.candidateOrder.join(',')}]`
    );
  }
  for (const row of evaluation.finalizationCoverage) {
    console.log(
      `  Tide ${row.epochNumber} active=${row.activePhaseUsers.length} missingManifest=[${row.missingManifestUsers.join(
        ','
      )}] missingFinalization=[${row.missingFinalizationUsers.join(',')}]`
    );
  }
  if (evaluation.blockers.length) {
    console.log(`Blockers (${evaluation.blockers.length}):`);
    for (const blocker of evaluation.blockers) console.log(`  ${blocker}`);
  }
}

async function main(): Promise<void> {
  const fixtureIndex = process.argv.indexOf('--fixture');
  let input: unknown;
  if (fixtureIndex >= 0) {
    const fixturePath = process.argv[fixtureIndex + 1];
    if (!fixturePath) throw new Error('--fixture requires a JSON path');
    input = readJsonFile(fixturePath);
  } else {
    input = await buildLiveInput(liveRuntimeFromEnvironment());
  }
  const evaluation = evaluateParityReport(input);
  printEvaluation(evaluation);
  process.exitCode = parityExitCode(evaluation);
}

function renderExecutionError(error: unknown): string {
  let rendered = error instanceof Error ? error.message : String(error);
  const configuredSecrets = [
    process.env.PROD_ADMIN_SECRET ?? '',
    process.env.LOCAL_ADMIN_SECRET ?? '',
  ].filter(secret => secret.length > 0);
  const secretRepresentations = new Set<string>();
  for (const secret of configuredSecrets) {
    secretRepresentations.add(secret);
    secretRepresentations.add(JSON.stringify(secret).slice(1, -1));
  }
  for (const secret of [...secretRepresentations].sort(
    (left, right) => right.length - left.length
  )) {
    if (secret) rendered = rendered.split(secret).join('[REDACTED]');
  }
  return rendered;
}

if (require.main === module) {
  main().catch(error => {
    console.error('parity check failed:', renderExecutionError(error));
    process.exitCode = 2;
  });
}
