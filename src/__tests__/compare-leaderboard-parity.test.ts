// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import {
  LEADERBOARD_PARITY_FIELD_MANIFEST,
  evaluateParityReport,
  parityExitCode,
  type ExactScalar,
  type ParityEvaluationInput,
} from '../../scripts/compare-leaderboard-parity';

const USER_A = '0x00000000000000000000000000000000000000a1';
const USER_B = '0x00000000000000000000000000000000000000b2';

type LiveMockRows = Record<string, unknown>;

const LIVE_MOCK_FIELD_MANIFEST = {
  UserPoints: LEADERBOARD_PARITY_FIELD_MANIFEST.UserPoints.map(rule => rule.field),
  UserLeaderboardState: LEADERBOARD_PARITY_FIELD_MANIFEST.UserLeaderboardState.map(
    rule => rule.field
  ),
  UserEpochStats: LEADERBOARD_PARITY_FIELD_MANIFEST.UserEpochStats.map(rule => rule.field),
  LeaderboardEpoch: [
    'id',
    'epochNumber',
    'startBlock',
    'startTime',
    'endBlock',
    'endTime',
    'isActive',
  ],
  LeaderboardKeeperUserSettled: ['id', 'user_id', 'epochNumber', 'isGap'],
  UserEpochFinalization: ['id', 'user_id', 'epochNumber'],
};

function emptyLiveRows(): LiveMockRows {
  return {
    UserPoints: [],
    UserLeaderboardState: [],
    UserEpochStats: [],
    LeaderboardEpoch: [],
    LeaderboardKeeperUserSettled: [],
    UserEpochFinalization: [],
    chain_metadata: [],
  };
}

type LiveAdapterOptions = {
  floor?: string;
  manifestPath?: string;
  prodUrl?: string | null;
  candidateUrl?: string | null;
  prodSecret?: string;
  candidateSecret?: string;
  expectedTarget?: string | null;
  requestTimeoutMs?: string;
  overallTimeoutMs?: string;
  maxPages?: string;
  maxRows?: string;
  mockFault?: string;
  mockFaultEntity?: string;
  mockFaultEndpoint?: string;
  spawnTimeoutMs?: number;
};

function runLiveAdapter(
  baseline: LiveMockRows,
  candidate: LiveMockRows,
  options: LiveAdapterOptions = {}
) {
  const fixtureDirectory = mkdtempSync(join(tmpdir(), 'parity-live-'));
  const fixturePath = join(fixtureDirectory, 'fixture.json');
  writeFileSync(fixturePath, JSON.stringify({ baseline, candidate }));
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PROD_GRAPHQL_URL: options.prodUrl ?? 'https://baseline.invalid/graphql',
    LOCAL_GRAPHQL_URL: options.candidateUrl ?? 'https://candidate.invalid/graphql',
    PROD_ADMIN_SECRET: options.prodSecret ?? '',
    LOCAL_ADMIN_SECRET: options.candidateSecret ?? '',
    PARITY_EXPECTED_TARGET_BLOCK: options.expectedTarget ?? '60670000',
    PARITY_REQUEST_TIMEOUT_MS: options.requestTimeoutMs ?? '',
    PARITY_OVERALL_TIMEOUT_MS: options.overallTimeoutMs ?? '',
    PARITY_MAX_PAGES: options.maxPages ?? '',
    PARITY_MAX_ROWS: options.maxRows ?? '',
    PARITY_LIVE_MOCK_JSON: '',
    PARITY_LIVE_MOCK_PATH: fixturePath,
    PARITY_LIVE_MOCK_FIELD_MANIFEST: JSON.stringify(LIVE_MOCK_FIELD_MANIFEST),
    PARITY_LIVE_MOCK_FAULT: options.mockFault ?? '',
    PARITY_LIVE_MOCK_FAULT_ENTITY: options.mockFaultEntity ?? '',
    PARITY_LIVE_MOCK_FAULT_ENDPOINT: options.mockFaultEndpoint ?? '',
    PARITY_LIVE_MOCK_EXPECTED_PROD_SECRET: options.prodSecret ?? '',
    PARITY_LIVE_MOCK_EXPECTED_CANDIDATE_SECRET: options.candidateSecret ?? '',
    PARITY_LP_CLASSIFICATIONS_PATH: '',
    PARITY_ARCHIVED_MANIFEST_PATH: options.manifestPath ?? '',
    PARITY_PROPOSED_FINAL_ONLY_FLOOR: options.floor ?? '',
    ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH: '',
  };
  if (options.prodUrl === null) delete env.PROD_GRAPHQL_URL;
  if (options.candidateUrl === null) delete env.LOCAL_GRAPHQL_URL;
  if (options.expectedTarget === null) delete env.PARITY_EXPECTED_TARGET_BLOCK;
  try {
    return spawnSync(
      process.execPath,
      [
        '--import',
        'tsx',
        '--import',
        './src/__tests__/parity-live-fetch-mock.ts',
        'scripts/compare-leaderboard-parity.ts',
      ],
      {
        cwd: process.cwd(),
        encoding: 'utf8',
        env,
        timeout: options.spawnTimeoutMs ?? 5_000,
      }
    );
  } finally {
    rmSync(fixtureDirectory, { recursive: true });
  }
}

function runLiveMockProbe(rows: LiveMockRows, query: string) {
  return spawnSync(
    process.execPath,
    [
      '--import',
      'tsx',
      '--import',
      './src/__tests__/parity-live-fetch-mock.ts',
      './src/__tests__/parity-live-mock-probe.ts',
    ],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
      timeout: 2_000,
      env: {
        ...process.env,
        PROD_GRAPHQL_URL: 'https://baseline.invalid/graphql',
        LOCAL_GRAPHQL_URL: 'https://candidate.invalid/graphql',
        PARITY_LIVE_MOCK_JSON: JSON.stringify({ baseline: rows, candidate: rows }),
        PARITY_LIVE_MOCK_PATH: '',
        PARITY_LIVE_MOCK_FIELD_MANIFEST: JSON.stringify(LIVE_MOCK_FIELD_MANIFEST),
        PARITY_LIVE_MOCK_PROBE_QUERY: query,
        PARITY_LIVE_MOCK_FAULT: '',
        PARITY_LIVE_MOCK_FAULT_ENTITY: '',
        PARITY_LIVE_MOCK_FAULT_ENDPOINT: '',
      },
    }
  );
}

type ExactRankingFixture = {
  epochNumber: ExactScalar;
  baselineScores: Array<{
    user: string;
    totalPoints: ExactScalar;
    totalPointsWithMultiplier: ExactScalar;
  }>;
  candidateScores: Array<{
    user: string;
    totalPoints: ExactScalar;
    totalPointsWithMultiplier: ExactScalar;
  }>;
};

type UserEpochFixture = {
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

type UserPointsFixture = {
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

type UserStateFixture = {
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

function epochFixture(overrides: Partial<UserEpochFixture> = {}): UserEpochFixture {
  return {
    id: `${USER_A}:8`,
    user_id: USER_A,
    epochNumber: '8',
    depositPoints: '10',
    borrowPoints: '20',
    lpPoints: '1000',
    dailySupplyPoints: '1',
    dailyBorrowPoints: '2',
    dailyRepayPoints: '3',
    dailyWithdrawPoints: '4',
    dailyVPPoints: '5',
    dailyLPPoints: '10',
    manualAwardPoints: '6',
    depositMultiplierBps: '10000',
    borrowMultiplierBps: '10000',
    vpMultiplierBps: '10000',
    lpMultiplierBps: '11000',
    depositPointsWithMultiplier: '15',
    borrowPointsWithMultiplier: '30',
    vpPointsWithMultiplier: '7',
    lpPointsWithMultiplier: '1100',
    lastSupplyPointsDay: 8,
    lastBorrowPointsDay: 8,
    lastRepayPointsDay: 8,
    lastWithdrawPointsDay: 8,
    lastVPPointsDay: 8,
    lastVPAccrualTimestamp: 800,
    totalPoints: '1061',
    totalPointsWithMultiplier: '1183',
    totalMultiplierBps: '11150',
    lastAppliedMultiplierBps: '11000',
    testnetBonusBps: '0',
    rank: 1,
    firstSeenAt: 100,
    lastUpdatedAt: 900,
    ...overrides,
  };
}

function userPointsFixture(overrides: Partial<UserPointsFixture> = {}): UserPointsFixture {
  return {
    id: USER_A,
    user_id: USER_A,
    lifetimeDepositPoints: '10',
    lifetimeBorrowPoints: '20',
    lifetimeDailySupplyPoints: '1',
    lifetimeDailyBorrowPoints: '2',
    lifetimeDailyRepayPoints: '3',
    lifetimeDailyWithdrawPoints: '4',
    lifetimeDailyVPPoints: '5',
    lifetimeTotalPoints: '1061',
    epochsParticipated: ['7', '8'],
    lifetimeEpochsIncluded: ['7', '8'],
    lastUpdatedAt: 900,
    ...overrides,
  };
}

function userStateFixture(overrides: Partial<UserStateFixture> = {}): UserStateFixture {
  return {
    id: USER_A,
    user_id: USER_A,
    nftCount: '1',
    nftMultiplier: '200',
    specialEditionCount: '1',
    specialEditionMultiplier: '300',
    votingPower: '1000',
    vpTierIndex: '2',
    vpMultiplier: '400',
    combinedMultiplier: '900',
    totalEpochsParticipated: '2',
    lifetimePoints: '1061',
    currentEpochId: `${USER_A}:8`,
    currentEpochRank: '1',
    lastUpdate: 900,
    ...overrides,
  };
}

function closedEpochFixture(epochNumber = '8') {
  const epoch = Number(epochNumber);
  return {
    id: epochNumber,
    epochNumber,
    startBlock: `${epoch * 100 - 99}`,
    startTime: epoch * 100 - 99,
    endBlock: `${epoch * 100}`,
    endTime: epoch * 100,
    isActive: false,
  };
}

function activeEpochFixture(epochNumber = '8') {
  const epoch = Number(epochNumber);
  return {
    id: epochNumber,
    epochNumber,
    startBlock: `${epoch * 100 - 99}`,
    startTime: epoch * 100 - 99,
    endBlock: null,
    endTime: null,
    isActive: true,
  };
}

function validLiveRows(): LiveMockRows {
  return {
    UserPoints: [userPointsFixture()],
    UserLeaderboardState: [userStateFixture()],
    UserEpochStats: [epochFixture()],
    LeaderboardEpoch: [closedEpochFixture()],
    LeaderboardKeeperUserSettled: [],
    UserEpochFinalization: [],
    chain_metadata: [{ chain_id: 143, latest_processed_block: '60670000' }],
  };
}

function validReportInput(): ParityEvaluationInput {
  return {
    exactScalarPrecision: true,
    nonLpDifferences: [],
    lpDifferences: [
      {
        user: USER_A,
        epochNumber: '8',
        component: 'lpPoints',
        baselinePoints: '1000',
        candidatePoints: '1002',
        computedRoundingBound: '2',
      },
      {
        user: USER_B,
        epochNumber: '8',
        component: 'lpPointsWithMultiplier',
        baselinePoints: '2000',
        candidatePoints: '2100',
        computedRoundingBound: '1',
        // `kind` is the discriminator; a bare `note` on an out-of-bound drift is blocked by
        // design so a comment alone can never wave a drift through.
        kind: 'MULTIPLIER_SEMANTIC',
        note: 'documented interval-average multiplier split',
      },
    ],
    closedTideRankings: [
      {
        epochNumber: '8',
        baselineScores: [
          {
            user: USER_A,
            totalPoints: '2200',
            totalPointsWithMultiplier: '2200',
          },
          {
            user: USER_B,
            totalPoints: '2100',
            totalPointsWithMultiplier: '2100',
          },
        ],
        candidateScores: [
          {
            user: USER_A.toUpperCase(),
            totalPoints: '2202',
            totalPointsWithMultiplier: '2202',
          },
          {
            user: USER_B,
            totalPoints: '2100',
            totalPointsWithMultiplier: '2100',
          },
        ],
      },
    ],
    finalizationCoverage: {
      proposedFloor: '8',
      baselineClosedTides: ['8'],
      candidateClosedTides: ['8'],
      requiredClosedTides: ['8'],
      tides: [
        {
          epochNumber: '8',
          activePhaseUsers: [USER_A, USER_B],
          finalizationUsers: [USER_A, USER_B.toUpperCase()],
        },
      ],
      manifestTides: [
        {
          epochNumber: '8',
          users: [USER_A.toUpperCase(), USER_B],
        },
      ],
    },
  };
}

test('offline parity evaluator enforces every Task 8 rollout blocker and exit verdict', async t => {
  await t.test('valid bounded and documented semantic drift passes', () => {
    const evaluation = evaluateParityReport(validReportInput());
    assert.equal(evaluation.verdict, 'PASS');
    assert.equal(parityExitCode(evaluation), 0);
    assert.deepEqual(
      evaluation.lpDifferences.map(row => [
        row.integerDrift,
        row.percentageDrift,
        row.classification,
      ]),
      [
        ['2', '+0.200000%', 'WITHIN_COMPUTED_BOUND'],
        ['100', '+5.000000%', 'MULTIPLIER_SEMANTIC'],
      ]
    );
  });

  const blockedCases: Array<{
    name: string;
    mutate: (input: ParityEvaluationInput) => void;
    blocker: RegExp;
  }> = [
    {
      name: 'exact non-LP mismatch',
      mutate: input => {
        input.nonLpDifferences.push({
          user: USER_A,
          epochNumber: '8',
          field: 'depositPoints',
          baseline: '10',
          candidate: '11',
        });
      },
      blocker: /NON_LP_MISMATCH/,
    },
    {
      name: 'out-of-bound unclassified LP drift',
      mutate: input => {
        input.lpDifferences[0].candidatePoints = '1003';
      },
      blocker: /LP_DRIFT_BLOCKED/,
    },
    {
      name: 'out-of-bound daily LP drift',
      mutate: input => {
        input.lpDifferences.push({
          user: USER_A,
          epochNumber: '8',
          component: 'dailyLPPoints',
          baselinePoints: '10',
          candidatePoints: '13',
          computedRoundingBound: '2',
        });
      },
      blocker: /LP_DRIFT_BLOCKED.*dailyLPPoints/,
    },
    {
      name: 'closed-Tide winner change',
      mutate: input => {
        const ranking = input.closedTideRankings[0] as unknown as ExactRankingFixture;
        ranking.candidateScores[0].totalPointsWithMultiplier = '2000';
        ranking.candidateScores[1].totalPointsWithMultiplier = '2300';
      },
      blocker: /CLOSED_TIDE_RANK_CHANGE.*winnerChanged=true/,
    },
    {
      name: 'lossy closed-Tide score',
      mutate: input => {
        const ranking = input.closedTideRankings[0] as unknown as ExactRankingFixture;
        ranking.candidateScores[0].totalPointsWithMultiplier = 2202;
      },
      blocker: /SCALAR_PRECISION_UNAVAILABLE.*totalPointsWithMultiplier/,
    },
    {
      name: 'missing archived-manifest user',
      mutate: input => {
        input.finalizationCoverage!.manifestTides![0].users = [USER_A];
      },
      blocker: /MANIFEST_USERS_MISSING.*00b2/,
    },
    {
      name: 'missing finalization user',
      mutate: input => {
        input.finalizationCoverage!.tides[0].finalizationUsers = [USER_A];
      },
      blocker: /FINALIZATION_USERS_MISSING.*00b2/,
    },
    {
      name: 'lossy BigInt scalar',
      mutate: input => {
        input.exactScalarPrecision = false;
        input.precisionFailures = ['candidate.UserEpochStats.lpPoints'];
        input.lpDifferences[0].candidatePoints = 1002;
      },
      blocker: /SCALAR_PRECISION_UNAVAILABLE/,
    },
  ];

  for (const blocked of blockedCases) {
    await t.test(blocked.name, () => {
      const input = validReportInput();
      blocked.mutate(input);
      const evaluation = evaluateParityReport(input);
      assert.equal(evaluation.verdict, 'BLOCK');
      assert.equal(parityExitCode(evaluation), 1);
      assert.match(evaluation.blockers.join('\n'), blocked.blocker);
    });
  }
});

test('offline snapshot adapter reports every LP-bearing field and exact closed-Tide score', async () => {
  const comparator = require('../../scripts/compare-leaderboard-parity') as Record<string, unknown>;
  const buildMaterial = comparator.buildUserEpochParityMaterial;
  assert.equal(
    typeof buildMaterial,
    'function',
    'the live adapter needs a pure fixture seam for exact UserEpochStats comparison'
  );

  const baseline = epochFixture();
  const candidate = epochFixture({
    dailyLPPoints: '13',
    totalPoints: '1064',
    totalPointsWithMultiplier: '1186',
  });
  const material = (
    buildMaterial as (
      baselineRows: UserEpochFixture[],
      candidateRows: UserEpochFixture[],
      closedEpochs: ExactScalar[]
    ) => Pick<ParityEvaluationInput, 'nonLpDifferences' | 'lpDifferences' | 'closedTideRankings'>
  )([baseline], [candidate], ['8']);

  assert.deepEqual(material.nonLpDifferences, []);
  assert.deepEqual(
    material.lpDifferences.map(row => row.component),
    ['dailyLPPoints', 'totalPoints', 'totalPointsWithMultiplier']
  );
  const ranking = material.closedTideRankings[0] as unknown as ExactRankingFixture;
  assert.deepEqual(ranking.baselineScores, [
    {
      user: USER_A,
      totalPoints: '1061',
      totalPointsWithMultiplier: '1183',
    },
  ]);
  assert.deepEqual(ranking.candidateScores, [
    {
      user: USER_A,
      totalPoints: '1064',
      totalPointsWithMultiplier: '1186',
    },
  ]);
});

test('Task 8 parity manifest reconciles every published field in the three score entities', async () => {
  const comparator = require('../../scripts/compare-leaderboard-parity') as Record<string, unknown>;
  const manifest = comparator.LEADERBOARD_PARITY_FIELD_MANIFEST as
    | Record<
        string,
        Array<{ field: string; category: string; reason?: string; comparison?: string }>
      >
    | undefined;
  assert.ok(manifest, 'the comparator must export its schema-backed parity field manifest');

  const schema = readFileSync('schema.graphql', 'utf8');
  for (const entity of ['UserPoints', 'UserLeaderboardState', 'UserEpochStats']) {
    const block = schema.match(new RegExp(`type ${entity} \\{([\\s\\S]*?)\\n\\}`));
    assert.ok(block, `${entity} schema block must exist`);
    const schemaFields = [...block[1].matchAll(/^\s{2}([A-Za-z0-9_]+):/gm)]
      .map(match => match[1])
      .sort();
    const rules: Array<{
      field: string;
      category: string;
      reason?: string;
      comparison?: string;
    }> = manifest[entity] ?? [];
    assert.deepEqual(
      rules.map(rule => rule.field).sort(),
      schemaFields,
      `${entity} manifest must classify every schema field exactly once`
    );
    assert.equal(new Set(rules.map(rule => rule.field)).size, rules.length);
    for (const rule of rules) {
      assert.match(rule.category, /^(IDENTITY|EXACT|LP_BEARING|EXCLUDED)$/);
      if (rule.category === 'EXCLUDED') {
        assert.ok(rule.reason?.trim(), `${entity}.${rule.field} exclusion needs a semantic reason`);
      }
      if (rule.category === 'LP_BEARING') {
        assert.match(rule.comparison ?? '', /^(DRIFT|CONSISTENCY)$/);
      }
    }
  }
});

test('full snapshot adapter blocks every exact field and surfaces every LP-bearing field', async t => {
  const comparator = require('../../scripts/compare-leaderboard-parity') as Record<string, unknown>;
  const buildMaterial = comparator.buildLeaderboardParityMaterial as
    | ((
        baseline: {
          userPoints: UserPointsFixture[];
          userLeaderboardStates: UserStateFixture[];
          userEpochStats: UserEpochFixture[];
        },
        candidate: {
          userPoints: UserPointsFixture[];
          userLeaderboardStates: UserStateFixture[];
          userEpochStats: UserEpochFixture[];
        },
        closedEpochs: ExactScalar[]
      ) => {
        preconditionFailures: string[];
        nonLpDifferences: ParityEvaluationInput['nonLpDifferences'];
        lpDifferences: ParityEvaluationInput['lpDifferences'];
        closedTideRankings: ParityEvaluationInput['closedTideRankings'];
      })
    | undefined;
  assert.equal(
    typeof buildMaterial,
    'function',
    'live comparison needs one pure adapter for UserPoints, UserLeaderboardState, and UserEpochStats'
  );

  const baseline = {
    userPoints: [userPointsFixture()],
    userLeaderboardStates: [userStateFixture()],
    userEpochStats: [epochFixture()],
  };
  const exactMutations: Array<{
    entity: keyof typeof baseline;
    field: string;
    value: unknown;
  }> = [
    { entity: 'userPoints', field: 'lifetimeDepositPoints', value: '11' },
    { entity: 'userPoints', field: 'lifetimeBorrowPoints', value: '21' },
    { entity: 'userPoints', field: 'lifetimeDailySupplyPoints', value: '2' },
    { entity: 'userPoints', field: 'lifetimeDailyBorrowPoints', value: '3' },
    { entity: 'userPoints', field: 'lifetimeDailyRepayPoints', value: '4' },
    { entity: 'userPoints', field: 'lifetimeDailyWithdrawPoints', value: '5' },
    { entity: 'userPoints', field: 'lifetimeDailyVPPoints', value: '6' },
    { entity: 'userPoints', field: 'epochsParticipated', value: ['7', '9'] },
    { entity: 'userPoints', field: 'lifetimeEpochsIncluded', value: ['7', '9'] },
    { entity: 'userLeaderboardStates', field: 'nftCount', value: '2' },
    { entity: 'userLeaderboardStates', field: 'nftMultiplier', value: '201' },
    { entity: 'userLeaderboardStates', field: 'specialEditionCount', value: '2' },
    {
      entity: 'userLeaderboardStates',
      field: 'specialEditionMultiplier',
      value: '301',
    },
    { entity: 'userLeaderboardStates', field: 'votingPower', value: '1001' },
    { entity: 'userLeaderboardStates', field: 'vpTierIndex', value: '3' },
    { entity: 'userLeaderboardStates', field: 'vpMultiplier', value: '401' },
    { entity: 'userLeaderboardStates', field: 'combinedMultiplier', value: '901' },
    { entity: 'userLeaderboardStates', field: 'totalEpochsParticipated', value: '3' },
    { entity: 'userLeaderboardStates', field: 'currentEpochId', value: `${USER_A}:9` },
    { entity: 'userEpochStats', field: 'depositPoints', value: '11' },
    { entity: 'userEpochStats', field: 'borrowPoints', value: '21' },
    { entity: 'userEpochStats', field: 'dailySupplyPoints', value: '2' },
    { entity: 'userEpochStats', field: 'dailyBorrowPoints', value: '3' },
    { entity: 'userEpochStats', field: 'dailyRepayPoints', value: '4' },
    { entity: 'userEpochStats', field: 'dailyWithdrawPoints', value: '5' },
    { entity: 'userEpochStats', field: 'dailyVPPoints', value: '6' },
    { entity: 'userEpochStats', field: 'manualAwardPoints', value: '7' },
    { entity: 'userEpochStats', field: 'depositMultiplierBps', value: '10001' },
    { entity: 'userEpochStats', field: 'borrowMultiplierBps', value: '10001' },
    { entity: 'userEpochStats', field: 'vpMultiplierBps', value: '10001' },
    { entity: 'userEpochStats', field: 'depositPointsWithMultiplier', value: '16' },
    { entity: 'userEpochStats', field: 'borrowPointsWithMultiplier', value: '31' },
    { entity: 'userEpochStats', field: 'vpPointsWithMultiplier', value: '8' },
    { entity: 'userEpochStats', field: 'lastSupplyPointsDay', value: 9 },
    { entity: 'userEpochStats', field: 'lastBorrowPointsDay', value: 9 },
    { entity: 'userEpochStats', field: 'lastRepayPointsDay', value: 9 },
    { entity: 'userEpochStats', field: 'lastWithdrawPointsDay', value: 9 },
    { entity: 'userEpochStats', field: 'lastVPPointsDay', value: 9 },
    { entity: 'userEpochStats', field: 'lastVPAccrualTimestamp', value: 801 },
    { entity: 'userEpochStats', field: 'testnetBonusBps', value: '1' },
  ];

  for (const mutation of exactMutations) {
    await t.test(`${mutation.entity}.${mutation.field}`, () => {
      const candidate = structuredClone(baseline) as typeof baseline;
      (candidate[mutation.entity][0] as unknown as Record<string, unknown>)[mutation.field] =
        mutation.value;
      const material = buildMaterial!(baseline, candidate, ['8']);
      const evaluation = evaluateParityReport({
        exactScalarPrecision: true,
        preconditionFailures: material.preconditionFailures,
        nonLpDifferences: material.nonLpDifferences,
        lpDifferences: material.lpDifferences,
        closedTideRankings: material.closedTideRankings,
      });
      assert.equal(evaluation.verdict, 'BLOCK');
      assert.match(evaluation.blockers.join('\n'), new RegExp(mutation.field));
    });
  }

  const lpMutations: Array<{
    entity: keyof typeof baseline;
    field: string;
    value: string;
    comparison: 'DRIFT' | 'CONSISTENCY';
  }> = [
    {
      entity: 'userPoints',
      field: 'lifetimeTotalPoints',
      value: '1062',
      comparison: 'DRIFT',
    },
    {
      entity: 'userLeaderboardStates',
      field: 'lifetimePoints',
      value: '1062',
      comparison: 'DRIFT',
    },
    { entity: 'userEpochStats', field: 'lpPoints', value: '1001', comparison: 'DRIFT' },
    {
      entity: 'userEpochStats',
      field: 'dailyLPPoints',
      value: '11',
      comparison: 'DRIFT',
    },
    {
      entity: 'userEpochStats',
      field: 'lpMultiplierBps',
      value: '11001',
      comparison: 'CONSISTENCY',
    },
    {
      entity: 'userEpochStats',
      field: 'lpPointsWithMultiplier',
      value: '1101',
      comparison: 'DRIFT',
    },
    { entity: 'userEpochStats', field: 'totalPoints', value: '1062', comparison: 'DRIFT' },
    {
      entity: 'userEpochStats',
      field: 'totalPointsWithMultiplier',
      value: '1184',
      comparison: 'DRIFT',
    },
    {
      entity: 'userEpochStats',
      field: 'totalMultiplierBps',
      value: '11151',
      comparison: 'CONSISTENCY',
    },
    {
      entity: 'userEpochStats',
      field: 'lastAppliedMultiplierBps',
      value: '11001',
      comparison: 'CONSISTENCY',
    },
  ];
  for (const mutation of lpMutations) {
    await t.test(`${mutation.entity}.${mutation.field}`, () => {
      const candidate = structuredClone(baseline) as typeof baseline;
      (candidate[mutation.entity][0] as unknown as Record<string, unknown>)[mutation.field] =
        mutation.value;
      const material = buildMaterial!(baseline, candidate, ['8']);
      if (mutation.comparison === 'DRIFT') {
        assert.ok(
          material.lpDifferences.some(row => row.component === mutation.field),
          `${mutation.field} must be emitted as classified LP drift material`
        );
      } else {
        assert.match(material.preconditionFailures.join('\n'), /LP_MATERIAL_MISMATCH/);
      }
      const evaluation = evaluateParityReport({
        exactScalarPrecision: true,
        preconditionFailures: material.preconditionFailures,
        nonLpDifferences: material.nonLpDifferences,
        lpDifferences: material.lpDifferences,
        closedTideRankings: material.closedTideRankings,
      });
      assert.equal(evaluation.verdict, 'BLOCK');
      assert.match(evaluation.blockers.join('\n'), new RegExp(mutation.field));
    });
  }
});

test('closed-Tide comparison blocks tie collapse and tie creation without address tie-breaking', async t => {
  const tieCases = [
    {
      name: 'tie collapse',
      baseline: ['100', '100'],
      candidate: ['101', '100'],
    },
    {
      name: 'tie creation',
      baseline: ['101', '100'],
      candidate: ['100', '100'],
    },
  ];
  for (const tieCase of tieCases) {
    await t.test(tieCase.name, () => {
      const input = validReportInput();
      input.lpDifferences = [
        {
          user: USER_A,
          epochNumber: '8',
          component: 'totalPointsWithMultiplier',
          baselinePoints: tieCase.baseline[0],
          candidatePoints: tieCase.candidate[0],
          computedRoundingBound: '1',
        },
      ];
      input.closedTideRankings = [
        {
          epochNumber: '8',
          baselineScores: [
            {
              user: USER_A,
              totalPoints: tieCase.baseline[0],
              totalPointsWithMultiplier: tieCase.baseline[0],
            },
            {
              user: USER_B,
              totalPoints: tieCase.baseline[1],
              totalPointsWithMultiplier: tieCase.baseline[1],
            },
          ],
          candidateScores: [
            {
              user: USER_A,
              totalPoints: tieCase.candidate[0],
              totalPointsWithMultiplier: tieCase.candidate[0],
            },
            {
              user: USER_B,
              totalPoints: tieCase.candidate[1],
              totalPointsWithMultiplier: tieCase.candidate[1],
            },
          ],
        },
      ];
      const evaluation = evaluateParityReport(input);
      assert.equal(evaluation.verdict, 'BLOCK');
      assert.equal(evaluation.rankChanges[0]?.winnerChanged, true);
      assert.match(evaluation.blockers.join('\n'), /CLOSED_TIDE_RANK_CHANGE/);
    });
  }

  await t.test('bounded drift preserving an exact tie cohort passes', () => {
    const input = validReportInput();
    input.lpDifferences = [
      {
        user: USER_A,
        epochNumber: '8',
        component: 'totalPointsWithMultiplier',
        baselinePoints: '100',
        candidatePoints: '101',
        computedRoundingBound: '1',
      },
      {
        user: USER_B,
        epochNumber: '8',
        component: 'totalPointsWithMultiplier',
        baselinePoints: '100',
        candidatePoints: '101',
        computedRoundingBound: '1',
      },
    ];
    input.closedTideRankings = [
      {
        epochNumber: '8',
        baselineScores: [
          { user: USER_A, totalPoints: '100', totalPointsWithMultiplier: '100' },
          { user: USER_B, totalPoints: '100', totalPointsWithMultiplier: '100' },
        ],
        candidateScores: [
          { user: USER_A, totalPoints: '101', totalPointsWithMultiplier: '101' },
          { user: USER_B, totalPoints: '101', totalPointsWithMultiplier: '101' },
        ],
      },
    ];
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'PASS');
    assert.deepEqual(evaluation.rankChanges, []);
  });
});

test('runtime validation rejects malformed, duplicate, and non-contiguous parity material', async t => {
  await t.test('malformed LP component and address', () => {
    const input = validReportInput();
    input.lpDifferences = [
      {
        user: 'not-an-address',
        epochNumber: '8',
        component: 'not-a-component',
        baselinePoints: '1',
        candidatePoints: '1',
        computedRoundingBound: '0',
      } as never,
    ];
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    assert.match(evaluation.blockers.join('\n'), /INPUT_INVALID.*(address|component)/i);
  });

  await t.test('duplicate finalization Tide rows cannot overwrite missing coverage', () => {
    const input = validReportInput();
    input.finalizationCoverage!.tides = [
      {
        epochNumber: '8',
        activePhaseUsers: [USER_A, USER_B],
        finalizationUsers: [USER_A],
      },
      input.finalizationCoverage!.tides[0],
    ];
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    assert.match(evaluation.blockers.join('\n'), /DUPLICATE.*Tide 8/i);
  });

  await t.test('duplicate manifest and finalization users block', () => {
    const input = validReportInput();
    input.finalizationCoverage!.manifestTides![0].users = [USER_A, USER_A.toUpperCase(), USER_B];
    input.finalizationCoverage!.tides[0].finalizationUsers = [USER_A, USER_B, USER_B.toUpperCase()];
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    assert.match(evaluation.blockers.join('\n'), /DUPLICATE.*(manifest|finalization).*user/i);
  });

  await t.test('duplicate manifest Tide rows block', () => {
    const input = validReportInput();
    input.finalizationCoverage!.manifestTides!.push({
      epochNumber: '8',
      users: [USER_A, USER_B],
    });
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    assert.match(evaluation.blockers.join('\n'), /DUPLICATE.*Tide 8/i);
  });

  await t.test('required closed Tide range is contiguous', () => {
    const input = validReportInput();
    input.finalizationCoverage!.baselineClosedTides = ['8', '10'];
    input.finalizationCoverage!.candidateClosedTides = ['8', '10'];
    input.finalizationCoverage!.requiredClosedTides = ['8', '10'];
    input.finalizationCoverage!.tides.push({
      epochNumber: '10',
      activePhaseUsers: [],
      finalizationUsers: [],
    });
    input.finalizationCoverage!.manifestTides!.push({ epochNumber: '10', users: [] });
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    assert.match(evaluation.blockers.join('\n'), /CLOSED_TIDE_RANGE.*9/i);
  });

  await t.test('classification JSON rejects invalid and duplicate keys', async () => {
    const comparator = require('../../scripts/compare-leaderboard-parity') as Record<
      string,
      unknown
    >;
    const validate = comparator.validateLPClassificationFile as
      | ((input: unknown) => { failures: string[] })
      | undefined;
    assert.equal(typeof validate, 'function');
    const result = validate!({
      rows: [
        {
          user: USER_A,
          epochNumber: '8',
          component: 'not-a-component',
          computedRoundingBound: 'bogus',
        },
        {
          user: USER_A,
          epochNumber: '8',
          component: 'lpPoints',
          computedRoundingBound: '1',
        },
        {
          user: USER_A.toUpperCase(),
          epochNumber: '08',
          component: 'lpPoints',
          computedRoundingBound: '1',
        },
      ],
    });
    assert.match(result.failures.join('\n'), /component/);
    assert.match(result.failures.join('\n'), /computedRoundingBound/);
    assert.match(result.failures.join('\n'), /duplicate/i);
  });

  await t.test('archived manifest JSON rejects malformed and duplicate material', async () => {
    const comparator = require('../../scripts/compare-leaderboard-parity') as Record<
      string,
      unknown
    >;
    const validate = comparator.validateArchivedManifestFile as
      | ((input: unknown) => { failures: string[] })
      | undefined;
    assert.equal(typeof validate, 'function');
    const result = validate!({
      tides: [
        { epochNumber: '8', users: [USER_A, USER_A.toUpperCase()] },
        { epochNumber: '08', users: [USER_B] },
        { epochNumber: 9, users: ['not-an-address'] },
      ],
    });
    const failures = result.failures.join('\n');
    assert.match(failures, /DUPLICATE manifest user/i);
    assert.match(failures, /duplicate Tide 8/i);
    assert.match(failures, /SCALAR_PRECISION_UNAVAILABLE/);
    assert.match(failures, /expected address/);
  });

  await t.test('malformed enum, array, negative bound, and duplicate score users block', () => {
    const input = validReportInput();
    input.lpDifferences[0].computedRoundingBound = '-1';
    (input.lpDifferences[0] as unknown as Record<string, unknown>).kind = 'UNKNOWN';
    input.closedTideRankings[0].candidateScores.push({
      user: USER_A.toUpperCase(),
      totalPoints: '2202',
      totalPointsWithMultiplier: '2202',
    });
    (input.finalizationCoverage as unknown as Record<string, unknown>).requiredClosedTides = '8';
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    const blockers = evaluation.blockers.join('\n');
    assert.match(blockers, /computedRoundingBound/);
    assert.match(blockers, /kind/);
    assert.match(blockers, /expected array/);
    assert.match(blockers, /duplicate user/i);
  });

  await t.test('endpoint, coverage, and manifest Tide sets reconcile exactly', () => {
    const input = validReportInput();
    input.finalizationCoverage!.baselineClosedTides = [];
    input.finalizationCoverage!.manifestTides = [];
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'BLOCK');
    const blockers = evaluation.blockers.join('\n');
    assert.match(blockers, /CLOSED_TIDE_ENDPOINT_MISMATCH baseline/);
    assert.match(blockers, /MANIFEST_TIDE_MISMATCH/);
  });

  await t.test('repeated raw active Keeper users remain an intentional set', () => {
    const input = validReportInput();
    input.finalizationCoverage!.tides[0].activePhaseUsers.push(USER_A.toUpperCase());
    const evaluation = evaluateParityReport(input);
    assert.equal(evaluation.verdict, 'PASS');
  });

  await t.test('duplicate normalized endpoint identities block before comparison', async () => {
    const comparator = require('../../scripts/compare-leaderboard-parity') as Record<
      string,
      unknown
    >;
    const buildMaterial = comparator.buildLeaderboardParityMaterial as
      | ((
          baseline: unknown,
          candidate: unknown,
          closedEpochs: ExactScalar[]
        ) => {
          preconditionFailures: string[];
        })
      | undefined;
    assert.equal(typeof buildMaterial, 'function');
    const duplicateSnapshots = [
      {
        name: 'UserPoints',
        snapshot: {
          userPoints: [
            userPointsFixture(),
            userPointsFixture({ id: USER_A.toUpperCase(), user_id: USER_A.toUpperCase() }),
          ],
          userLeaderboardStates: [userStateFixture()],
          userEpochStats: [epochFixture()],
        },
      },
      {
        name: 'UserLeaderboardState',
        snapshot: {
          userPoints: [userPointsFixture()],
          userLeaderboardStates: [
            userStateFixture(),
            userStateFixture({ id: USER_A.toUpperCase(), user_id: USER_A.toUpperCase() }),
          ],
          userEpochStats: [epochFixture()],
        },
      },
      {
        name: 'UserEpochStats',
        snapshot: {
          userPoints: [userPointsFixture()],
          userLeaderboardStates: [userStateFixture()],
          userEpochStats: [
            epochFixture(),
            epochFixture({
              id: `${USER_A.toUpperCase()}:8`,
              user_id: USER_A.toUpperCase(),
            }),
          ],
        },
      },
    ];
    for (const duplicate of duplicateSnapshots) {
      const material = buildMaterial!(duplicate.snapshot, duplicate.snapshot, ['8']);
      assert.match(
        material.preconditionFailures.join('\n'),
        new RegExp(`DUPLICATE_ENDPOINT_IDENTITY .*${duplicate.name}`)
      );
    }
  });
});

test('offline fixture CLI returns deterministic zero and one exit statuses without network', () => {
  const fixtureSecretFragments = ['FIXTURE_PROD_SECRET', 'FIXTURE_CANDIDATE_SECRET'];
  const runFixture = (path: string) =>
    spawnSync(
      process.execPath,
      [
        '--import',
        'tsx',
        '--import',
        './src/__tests__/parity-fetch-forbidden.ts',
        'scripts/compare-leaderboard-parity.ts',
        '--fixture',
        path,
      ],
      {
        cwd: process.cwd(),
        encoding: 'utf8',
        env: {
          ...process.env,
          PROD_ADMIN_SECRET: `${fixtureSecretFragments[0]}\nSECOND_LINE`,
          LOCAL_ADMIN_SECRET: `${fixtureSecretFragments[1]}\rTRAILING_LINE`,
        },
      }
    );

  const valid = runFixture('src/__tests__/fixtures/parity-valid.json');
  assert.equal(valid.status, 0, valid.stderr || valid.stdout);
  assert.match(valid.stdout, /Rollout parity verdict: PASS/);

  const blocked = runFixture('src/__tests__/fixtures/parity-blocked.json');
  assert.equal(blocked.status, 1, blocked.stderr || blocked.stdout);
  assert.match(blocked.stdout, /Rollout parity verdict: BLOCK/);
  assert.match(blocked.stdout, /NON_LP_MISMATCH/);

  const invalid = runFixture('src/__tests__/fixtures/parity-invalid.json');
  assert.equal(invalid.status, 1, invalid.stderr || invalid.stdout);
  assert.match(invalid.stdout, /Rollout parity verdict: BLOCK/);
  assert.match(invalid.stdout, /INPUT_INVALID/);

  const malformed = runFixture('src/__tests__/fixtures/parity-malformed.txt');
  assert.equal(malformed.status, 2, malformed.stderr || malformed.stdout);
  assert.match(malformed.stderr, /parity check failed/);
  assert.doesNotMatch(malformed.stderr, /fixture mode must not call fetch/);
  for (const result of [valid, blocked, invalid, malformed]) {
    const output = `${result.stdout}\n${result.stderr}`;
    for (const fragment of fixtureSecretFragments) {
      assert.doesNotMatch(output, new RegExp(fragment));
    }
  }
});

test('live adapter preserves above-floor archive, raw, and finalization Tide evidence', () => {
  const baseline = validLiveRows();
  const candidate = validLiveRows();
  const closedTide8 = closedEpochFixture('8');
  baseline.LeaderboardEpoch = [closedTide8];
  candidate.LeaderboardEpoch = [closedTide8];
  candidate.LeaderboardKeeperUserSettled = [
    { id: 'raw-9', user_id: USER_A, epochNumber: '9', isGap: false },
  ];
  candidate.UserEpochFinalization = [{ id: `${USER_A}:9`, user_id: USER_A, epochNumber: '9' }];

  const result = runLiveAdapter(baseline, candidate, {
    floor: '8',
    manifestPath: 'src/__tests__/fixtures/parity-live-manifest-8-9.json',
  });

  assert.equal(result.status, 1, result.stderr || result.stdout);
  assert.match(result.stdout, /Rollout parity verdict: BLOCK/);
  assert.match(result.stdout, /CLOSED_TIDE_ENDPOINT_MISMATCH.*9/);
});

test('live adapter accepts canonical active epochs with nullable closure fields', () => {
  const baseline = validLiveRows();
  const candidate = validLiveRows();
  const activeTide8 = activeEpochFixture('8');
  baseline.LeaderboardEpoch = [activeTide8];
  candidate.LeaderboardEpoch = [activeTide8];

  const result = runLiveAdapter(baseline, candidate);

  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stdout, /Rollout parity verdict: PASS/);
  assert.doesNotMatch(result.stdout, /SCALAR_PRECISION_UNAVAILABLE/);
});

test('live adapter exactly reconciles the unfiltered above-floor Tide union', async t => {
  const raw = (epochNumber: string) => ({
    id: `raw-${epochNumber}`,
    user_id: USER_A,
    epochNumber,
    isGap: false,
  });
  const finalization = (epochNumber: string) => ({
    id: `${USER_A}:${epochNumber}`,
    user_id: USER_A,
    epochNumber,
  });

  await t.test('complete endpoint, coverage, and manifest sources through Tide 9 pass', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('9')];
    candidate.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('9')];
    candidate.LeaderboardKeeperUserSettled = [raw('9')];
    candidate.UserEpochFinalization = [finalization('9')];

    const result = runLiveAdapter(baseline, candidate, {
      floor: '8',
      manifestPath: 'src/__tests__/fixtures/parity-live-manifest-8-9.json',
    });

    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.match(result.stdout, /Rollout parity verdict: PASS/);
    assert.match(result.stdout, /Tide 9 active=1/);
  });

  await t.test('an internal closed-Tide gap blocks', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('10')];
    candidate.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('10')];

    const result = runLiveAdapter(baseline, candidate, {
      floor: '8',
      manifestPath: 'src/__tests__/fixtures/parity-live-manifest-8-10.json',
    });

    assert.equal(result.status, 1, result.stderr || result.stdout);
    assert.match(result.stdout, /Rollout parity verdict: BLOCK/);
    assert.match(result.stdout, /CLOSED_TIDE_(RANGE|ENDPOINT)_MISMATCH.*9/);
  });

  for (const evidence of ['raw', 'finalization'] as const) {
    await t.test(`extra ${evidence}-only Tide evidence blocks`, () => {
      const baseline = validLiveRows();
      const candidate = validLiveRows();
      if (evidence === 'raw') candidate.LeaderboardKeeperUserSettled = [raw('9')];
      if (evidence === 'finalization') {
        candidate.UserEpochFinalization = [finalization('9')];
      }

      const result = runLiveAdapter(baseline, candidate, {
        floor: '8',
        manifestPath: 'src/__tests__/fixtures/parity-live-manifest-8.json',
      });

      assert.equal(result.status, 1, result.stderr || result.stdout);
      assert.match(result.stdout, /Rollout parity verdict: BLOCK/);
      assert.match(result.stdout, /CLOSED_TIDE_ENDPOINT_MISMATCH.*9/);
    });
  }

  await t.test('below-floor manifest, raw, and finalization evidence is ignored', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = [closedEpochFixture('7'), closedEpochFixture('8')];
    candidate.LeaderboardEpoch = [closedEpochFixture('7'), closedEpochFixture('8')];
    candidate.LeaderboardKeeperUserSettled = [raw('7')];
    candidate.UserEpochFinalization = [finalization('7')];

    const result = runLiveAdapter(baseline, candidate, {
      floor: '8',
      manifestPath: 'src/__tests__/fixtures/parity-live-manifest-7-8.json',
    });

    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.match(result.stdout, /Rollout parity verdict: PASS/);
    assert.doesNotMatch(result.stdout, /Tide 7 active=/);
  });
});

test('live adapter still requires an exact endBlock for a closed epoch', () => {
  const baseline = validLiveRows();
  const candidate = validLiveRows();
  const invalidClosedTide8 = { ...closedEpochFixture('8'), endBlock: null };
  baseline.LeaderboardEpoch = [invalidClosedTide8];
  candidate.LeaderboardEpoch = [invalidClosedTide8];

  const result = runLiveAdapter(baseline, candidate);

  assert.equal(result.status, 1, result.stderr || result.stdout);
  assert.match(result.stdout, /Rollout parity verdict: BLOCK/);
  assert.match(result.stdout, /(?:SCALAR_PRECISION_UNAVAILABLE|EPOCH_MATERIAL_INVALID).*endBlock/);
});

test('live collection is bounded and validates every pagination page', async t => {
  const epochs = (count: number) =>
    Array.from({ length: count }, (_, index) => closedEpochFixture(String(index + 1)));

  await t.test('1,001 rows are fetched in exactly two pages', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = epochs(1_001);
    candidate.LeaderboardEpoch = epochs(1_001);

    const result = runLiveAdapter(baseline, candidate, { spawnTimeoutMs: 2_000 });

    assert.equal(result.status, 0, result.stderr || result.stdout || String(result.error));
    assert.match(result.stdout, /Rollout parity verdict: PASS/);
  });

  await t.test('a repeated full page exits 2 with a cursor diagnostic', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = epochs(1_000);
    candidate.LeaderboardEpoch = epochs(1_000);

    const result = runLiveAdapter(baseline, candidate, {
      mockFault: 'repeat-first-page',
      mockFaultEntity: 'LeaderboardEpoch',
      mockFaultEndpoint: 'candidate',
      spawnTimeoutMs: 1_500,
    });

    assert.equal(result.status, 2, result.stderr || result.stdout || String(result.error));
    assert.match(result.stderr, /cursor|strictly increasing|pagination/i);
  });

  const malformedPages = [
    { name: 'descending IDs', fault: 'descending' },
    { name: 'duplicate IDs', fault: 'duplicate-id' },
    { name: 'missing ID', fault: 'missing-id' },
    { name: 'non-string ID', fault: 'non-string-id' },
    { name: 'non-array payload', fault: 'non-array' },
    { name: 'oversized page', fault: 'oversized' },
  ];
  for (const malformed of malformedPages) {
    await t.test(`${malformed.name} exits 2`, () => {
      const baseline = validLiveRows();
      const candidate = validLiveRows();
      baseline.LeaderboardEpoch = malformed.fault === 'oversized' ? epochs(1_001) : epochs(2);
      candidate.LeaderboardEpoch = malformed.fault === 'oversized' ? epochs(1_001) : epochs(2);

      const result = runLiveAdapter(baseline, candidate, {
        mockFault: malformed.fault,
        mockFaultEntity: 'LeaderboardEpoch',
        mockFaultEndpoint: 'candidate',
        spawnTimeoutMs: 1_500,
      });

      assert.equal(result.status, 2, result.stderr || result.stdout || String(result.error));
      assert.match(result.stderr, /page|pagination|array|id/i);
    });
  }

  await t.test('row and page ceilings exit 2', () => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = epochs(1_001);
    candidate.LeaderboardEpoch = epochs(1_001);

    const rowLimited = runLiveAdapter(baseline, candidate, {
      maxRows: '1000',
      spawnTimeoutMs: 1_500,
    });
    assert.equal(
      rowLimited.status,
      2,
      rowLimited.stderr || rowLimited.stdout || String(rowLimited.error)
    );
    assert.match(rowLimited.stderr, /row ceiling/i);

    const pageLimited = runLiveAdapter(baseline, candidate, {
      maxPages: '1',
      spawnTimeoutMs: 1_500,
    });
    assert.equal(
      pageLimited.status,
      2,
      pageLimited.stderr || pageLimited.stdout || String(pageLimited.error)
    );
    assert.match(pageLimited.stderr, /page ceiling/i);
  });

  for (const deadline of [
    { name: 'request', requestTimeoutMs: '100', overallTimeoutMs: '1000' },
    { name: 'overall', requestTimeoutMs: '1000', overallTimeoutMs: '100' },
  ]) {
    await t.test(`a never-resolving fetch obeys the ${deadline.name} deadline`, () => {
      const startedAt = Date.now();
      const result = runLiveAdapter(validLiveRows(), validLiveRows(), {
        mockFault: 'never-resolve',
        mockFaultEntity: 'LeaderboardEpoch',
        mockFaultEndpoint: 'candidate',
        requestTimeoutMs: deadline.requestTimeoutMs,
        overallTimeoutMs: deadline.overallTimeoutMs,
        spawnTimeoutMs: 1_500,
      });
      const elapsedMs = Date.now() - startedAt;

      assert.equal(result.status, 2, result.stderr || result.stdout || String(result.error));
      assert.match(result.stderr, /timed out|deadline|abort/i);
      assert.ok(elapsedMs < 1_500, `deadline took ${elapsedMs}ms`);
    });
  }
});

test('live configuration and fixed-target evidence are mandatory', async t => {
  const configCases: Array<{
    name: string;
    options: LiveAdapterOptions;
    diagnostic: RegExp;
  }> = [
    {
      name: 'missing baseline URL',
      options: { prodUrl: null },
      diagnostic: /PROD_GRAPHQL_URL.*required/i,
    },
    {
      name: 'invalid candidate URL',
      options: { candidateUrl: 'not a URL' },
      diagnostic: /LOCAL_GRAPHQL_URL.*valid/i,
    },
    {
      name: 'normalized-equal URLs',
      options: {
        prodUrl: 'https://SAME.invalid/graphql',
        candidateUrl: 'https://same.invalid/graphql',
      },
      diagnostic: /distinct/i,
    },
    {
      name: 'missing expected target',
      options: { expectedTarget: null },
      diagnostic: /PARITY_EXPECTED_TARGET_BLOCK.*required/i,
    },
    {
      name: 'zero expected target',
      options: { expectedTarget: '0' },
      diagnostic: /PARITY_EXPECTED_TARGET_BLOCK.*positive/i,
    },
    {
      name: 'invalid request timeout',
      options: { requestTimeoutMs: '0' },
      diagnostic: /PARITY_REQUEST_TIMEOUT_MS.*positive/i,
    },
    {
      name: 'invalid overall timeout',
      options: { overallTimeoutMs: 'not-a-number' },
      diagnostic: /PARITY_OVERALL_TIMEOUT_MS.*positive/i,
    },
  ];
  for (const item of configCases) {
    await t.test(`${item.name} exits 2`, () => {
      const result = runLiveAdapter(validLiveRows(), validLiveRows(), item.options);
      assert.equal(result.status, 2, result.stderr || result.stdout);
      assert.match(result.stderr, item.diagnostic);
    });
  }

  await t.test('fully populated material at the explicit target passes', () => {
    const result = runLiveAdapter(validLiveRows(), validLiveRows());
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.match(result.stdout, /Rollout parity verdict: PASS/);
  });

  await t.test('empty leaderboard material blocks', () => {
    const baseline = emptyLiveRows();
    const candidate = emptyLiveRows();
    baseline.chain_metadata = [{ chain_id: 143, latest_processed_block: '60670000' }];
    candidate.chain_metadata = [{ chain_id: 143, latest_processed_block: '60670000' }];
    const result = runLiveAdapter(baseline, candidate);
    assert.equal(result.status, 1, result.stderr || result.stdout);
    assert.match(result.stdout, /SNAPSHOT_MATERIAL_EMPTY/);
  });

  const targetCases = [
    { name: 'missing metadata', rows: [] },
    { name: 'zero progress', rows: [{ chain_id: 143, latest_processed_block: '0' }] },
    {
      name: 'wrong chain',
      rows: [{ chain_id: 1, latest_processed_block: '60670000' }],
    },
    {
      name: 'target mismatch',
      rows: [{ chain_id: 143, latest_processed_block: '60669999' }],
    },
    {
      name: 'malformed target',
      rows: [{ chain_id: 143, latest_processed_block: 60670000.5 }],
    },
  ];
  for (const item of targetCases) {
    await t.test(`${item.name} chain metadata blocks with exit 1`, () => {
      const candidate = validLiveRows();
      candidate.chain_metadata = item.rows;
      const result = runLiveAdapter(validLiveRows(), candidate);
      assert.equal(result.status, 1, result.stderr || result.stdout);
      assert.match(result.stdout, /CHAIN_METADATA|TARGET_BLOCK/);
    });
  }
});

test('live admin secrets are never rendered when header validation fails', () => {
  const cases = [
    {
      variable: 'PROD_ADMIN_SECRET',
      options: { prodSecret: 'PROD_SECRET_SENTINEL_6b3f\nSECOND_LINE' },
      fragments: ['PROD_SECRET_SENTINEL_6b3f', 'SECOND_LINE'],
    },
    {
      variable: 'LOCAL_ADMIN_SECRET',
      options: { candidateSecret: 'CANDIDATE_SECRET_SENTINEL_7c4a\rTRAILING_LINE' },
      fragments: ['CANDIDATE_SECRET_SENTINEL_7c4a', 'TRAILING_LINE'],
    },
  ];

  for (const item of cases) {
    const result = runLiveAdapter(validLiveRows(), validLiveRows(), item.options);
    const output = `${result.stdout}\n${result.stderr}`;
    assert.equal(result.status, 2, output);
    assert.match(result.stderr, new RegExp(`${item.variable}.*invalid`, 'i'));
    for (const fragment of item.fragments) assert.doesNotMatch(output, new RegExp(fragment));
  }
});

test('valid live admin secrets are redacted from reflected transport errors', () => {
  const prodSecret = 'VALID_PROD_ADMIN_SECRET_SENTINEL_4a9c';
  const candidateSecret = 'VALID_CANDIDATE_ADMIN_SECRET_SENTINEL_8d2e';
  const result = runLiveAdapter(validLiveRows(), validLiveRows(), {
    prodSecret,
    candidateSecret,
    mockFault: 'echo-secret',
    mockFaultEntity: 'LeaderboardEpoch',
    mockFaultEndpoint: 'baseline',
  });
  const output = `${result.stdout}\n${result.stderr}`;

  assert.equal(result.status, 2, output);
  assert.match(result.stderr, /mock transport echoed \[REDACTED\]/i);
  assert.doesNotMatch(output, new RegExp(prodSecret));
  assert.doesNotMatch(output, new RegExp(candidateSecret));
});

test('valid live admin secrets remain headers-only across verdict and error paths', () => {
  const prodSecret = 'VALID_PROD_HEADER_SENTINEL_1f3b';
  const candidateSecret = 'VALID_CANDIDATE_HEADER_SENTINEL_5d7a';
  const commonOptions = { prodSecret, candidateSecret };
  const runs: Array<{ name: string; result: ReturnType<typeof runLiveAdapter> }> = [];

  runs.push({
    name: 'PASS',
    result: runLiveAdapter(validLiveRows(), validLiveRows(), commonOptions),
  });

  const blockedCandidate = validLiveRows();
  blockedCandidate.chain_metadata = [{ chain_id: 143, latest_processed_block: '60669999' }];
  runs.push({
    name: 'BLOCK',
    result: runLiveAdapter(validLiveRows(), blockedCandidate, commonOptions),
  });

  runs.push({
    name: 'timeout',
    result: runLiveAdapter(validLiveRows(), validLiveRows(), {
      ...commonOptions,
      mockFault: 'never-resolve',
      mockFaultEntity: 'LeaderboardEpoch',
      mockFaultEndpoint: 'baseline',
      requestTimeoutMs: '100',
      overallTimeoutMs: '1000',
      spawnTimeoutMs: 1_500,
    }),
  });

  runs.push({
    name: 'GraphQL error',
    result: runLiveAdapter(validLiveRows(), validLiveRows(), {
      ...commonOptions,
      mockFault: 'graphql-error-secret',
      mockFaultEntity: 'LeaderboardEpoch',
      mockFaultEndpoint: 'candidate',
    }),
  });

  assert.deepEqual(
    runs.map(run => run.result.status),
    [0, 1, 2, 2],
    runs.map(run => `${run.name}: ${run.result.stderr || run.result.stdout}`).join('\n')
  );
  assert.match(runs[3].result.stderr, /mock GraphQL echoed \[REDACTED\]/i);
  for (const run of runs) {
    const output = `${run.result.stdout}\n${run.result.stderr}`;
    assert.doesNotMatch(output, new RegExp(prodSecret), run.name);
    assert.doesNotMatch(output, new RegExp(candidateSecret), run.name);
  }
});

test('live epoch material must be canonical and internally coherent', async t => {
  const invalidEpochs: Array<{ name: string; row: Record<string, unknown> }> = [
    { name: 'wrong ID', row: { ...closedEpochFixture(), id: 'not-8' } },
    { name: 'numeric alias', row: { ...closedEpochFixture(), id: '08', epochNumber: '08' } },
    { name: 'zero epoch', row: { ...closedEpochFixture(), id: '0', epochNumber: '0' } },
    { name: 'zero endBlock', row: { ...closedEpochFixture(), endBlock: '0' } },
    { name: 'zero endTime', row: { ...closedEpochFixture(), endTime: 0 } },
    { name: 'negative endBlock', row: { ...closedEpochFixture(), endBlock: '-1' } },
    { name: 'negative endTime', row: { ...closedEpochFixture(), endTime: -1 } },
    { name: 'active with closure', row: { ...closedEpochFixture(), isActive: true } },
    { name: 'inactive without closure', row: { ...activeEpochFixture(), isActive: false } },
    { name: 'endBlock without endTime', row: { ...activeEpochFixture(), endBlock: '800' } },
    { name: 'endTime without endBlock', row: { ...closedEpochFixture(), endBlock: null } },
    { name: 'malformed activity', row: { ...activeEpochFixture(), isActive: 'true' } },
    { name: 'zero startBlock', row: { ...closedEpochFixture(), startBlock: '0' } },
    { name: 'zero startTime', row: { ...closedEpochFixture(), startTime: 0 } },
    { name: 'end before start', row: { ...closedEpochFixture(), endTime: 1 } },
  ];
  for (const item of invalidEpochs) {
    await t.test(`${item.name} blocks with exit 1`, () => {
      const baseline = validLiveRows();
      const candidate = validLiveRows();
      baseline.LeaderboardEpoch = [item.row];
      candidate.LeaderboardEpoch = [item.row];
      const result = runLiveAdapter(baseline, candidate);
      assert.equal(result.status, 1, result.stderr || result.stdout);
      assert.match(result.stdout, /EPOCH_MATERIAL_INVALID|SCALAR_PRECISION_UNAVAILABLE/);
    });
  }

  for (const item of [
    { name: 'canonical closed', row: closedEpochFixture() },
    { name: 'canonical open', row: activeEpochFixture() },
  ]) {
    await t.test(`${item.name} epoch passes`, () => {
      const baseline = validLiveRows();
      const candidate = validLiveRows();
      baseline.LeaderboardEpoch = [item.row];
      candidate.LeaderboardEpoch = [item.row];
      const result = runLiveAdapter(baseline, candidate);
      assert.equal(result.status, 0, result.stderr || result.stdout);
      assert.match(result.stdout, /Rollout parity verdict: PASS/);
    });
  }
});

test('live finalization coverage requires canonical certificate identities', async t => {
  const runFinalization = (id: string, user = USER_A) => {
    const baseline = validLiveRows();
    const candidate = validLiveRows();
    baseline.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('9')];
    candidate.LeaderboardEpoch = [closedEpochFixture('8'), closedEpochFixture('9')];
    candidate.LeaderboardKeeperUserSettled = [
      { id: 'raw-9', user_id: USER_A, epochNumber: '9', isGap: false },
    ];
    candidate.UserEpochFinalization = [{ id, user_id: user, epochNumber: '9' }];
    return runLiveAdapter(baseline, candidate, {
      floor: '8',
      manifestPath: 'src/__tests__/fixtures/parity-live-manifest-8-9.json',
    });
  };

  for (const item of [
    { name: 'unrelated ID', id: 'wrong-certificate-id' },
    { name: 'wrong address', id: `${USER_B}:9` },
    { name: 'wrong Tide', id: `${USER_A}:8` },
    { name: 'aliased Tide', id: `${USER_A}:09` },
  ]) {
    await t.test(`${item.name} blocks`, () => {
      const result = runFinalization(item.id);
      assert.equal(result.status, 1, result.stderr || result.stdout);
      assert.match(result.stdout, /FINALIZATION_IDENTITY_INVALID/);
    });
  }

  for (const id of [`${USER_A}:9`, `${USER_A.toUpperCase()}:9`]) {
    await t.test(`canonical identity ${id} passes`, () => {
      const result = runFinalization(id);
      assert.equal(result.status, 0, result.stderr || result.stdout);
      assert.match(result.stdout, /Rollout parity verdict: PASS/);
    });
  }
});

test('live GraphQL mock enforces pagination, projection, and Keeper filtering', async t => {
  const fields = LEADERBOARD_PARITY_FIELD_MANIFEST.UserPoints.map(rule => rule.field);
  const baseQuery = (selectedFields: string) =>
    `query { UserPoints(where: { id: { _gt: ${JSON.stringify(
      USER_A
    )} } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } }`;
  const paginatedQuery = (entity: keyof typeof LIVE_MOCK_FIELD_MANIFEST) => {
    const keeperPredicate =
      entity === 'LeaderboardKeeperUserSettled' ? ', isGap: { _eq: false }' : '';
    return `query { ${entity}(where: { id: { _gt: "" }${keeperPredicate} }, order_by: { id: asc }, limit: 1000) { ${LIVE_MOCK_FIELD_MANIFEST[
      entity
    ].join(' ')} } }`;
  };
  const rows = validLiveRows();
  rows.UserPoints = [
    { ...userPointsFixture(), poison: 'must-not-project' },
    userPointsFixture({ id: USER_B, user_id: USER_B }),
  ];

  await t.test('cursor and selected-field projection are honored', () => {
    const result = runLiveMockProbe(rows, baseQuery(fields.join(' ')));
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.doesNotMatch(result.stdout, /poison|must-not-project/);
    const payload = JSON.parse(result.stdout) as { data: { UserPoints: unknown[] } };
    assert.equal(payload.data.UserPoints.length, 1);
    assert.deepEqual(Object.keys(payload.data.UserPoints[0] as object).sort(), [...fields].sort());
    assert.equal((payload.data.UserPoints[0] as { id: string }).id, USER_B);
  });

  await t.test('unsupported pagination arguments and predicates exit 2', () => {
    const query = `query { UserPoints(where: { id: { _gt: "" }, user_id: { _eq: ${JSON.stringify(
      USER_A
    )} } }, order_by: { id: asc }, offset: 1, limit: 1000) { ${fields.join(' ')} } }`;
    const result = runLiveMockProbe(rows, query);

    assert.equal(result.status, 2, result.stderr || result.stdout);
    assert.match(result.stderr, /unsupported.*query.*shape|argument|predicate/i);
  });

  await t.test('unsupported Keeper predicates exit 2', () => {
    const query = `query { LeaderboardKeeperUserSettled(where: { id: { _gt: "" }, isGap: { _eq: false }, user_id: { _eq: ${JSON.stringify(
      USER_A
    )} } }, order_by: { id: asc }, limit: 1000) { ${LIVE_MOCK_FIELD_MANIFEST.LeaderboardKeeperUserSettled.join(
      ' '
    )} } }`;
    const result = runLiveMockProbe(rows, query);

    assert.equal(result.status, 2, result.stderr || result.stdout);
    assert.match(result.stderr, /unsupported.*query.*shape/i);
  });

  await t.test('every supported entity query shape is accepted', () => {
    const supportedQueries = [
      paginatedQuery('UserPoints'),
      paginatedQuery('UserLeaderboardState'),
      paginatedQuery('UserEpochStats'),
      paginatedQuery('LeaderboardEpoch'),
      paginatedQuery('LeaderboardKeeperUserSettled'),
      paginatedQuery('UserEpochFinalization'),
      `query { chain_metadata(where: { chain_id: { _eq: 143 } }, limit: 1) { chain_id latest_processed_block } }`,
      `query { UserLeaderboardState(order_by: { lastUpdate: desc }, limit: 1) { lastUpdate } }`,
    ];

    for (const query of supportedQueries) {
      const result = runLiveMockProbe(rows, query);
      assert.equal(result.status, 0, `${query}\n${result.stderr || result.stdout}`);
    }
  });

  await t.test(
    'duplicate, unknown, aliased, directive-bearing, and trailing query material exits 2',
    () => {
      const selectedFields = fields.join(' ');
      const unsupportedQueries = [
        `query { UserPoints(where: { id: { _gt: "" }, id: { _gt: "" } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } }`,
        `query { UserPoints(where: { id: { _gt: "" }, unknown: { _eq: "x" } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } }`,
        `query { UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc, user_id: asc }, limit: 1000) { ${selectedFields} } }`,
        `query { UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc }, limit: 1000, limit: 1000) { ${selectedFields} } }`,
        `query { rows: UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } }`,
        `query { UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc }, limit: 1000) @skip(if: false) { ${selectedFields} } }`,
        `query { UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } } query { UserPoints(where: { id: { _gt: "" } }, order_by: { id: asc }, limit: 1000) { ${selectedFields} } }`,
        `query { chain_metadata(where: { chain_id: { _eq: 143 } }, offset: 1, limit: 1) { chain_id latest_processed_block } }`,
        `query { UserLeaderboardState(where: { id: { _gt: "" } }, order_by: { lastUpdate: desc }, limit: 1) { lastUpdate } }`,
      ];

      for (const query of unsupportedQueries) {
        const result = runLiveMockProbe(rows, query);
        assert.equal(result.status, 2, `${query}\n${result.stderr || result.stdout}`);
        assert.match(result.stderr, /unsupported.*query.*shape/i, query);
      }
    }
  );

  for (const item of [
    { name: 'unknown selection', fields: [...fields, 'poison'] },
    { name: 'omitted selection', fields: fields.slice(0, -1) },
  ]) {
    await t.test(`${item.name} exits 2`, () => {
      const result = runLiveMockProbe(rows, baseQuery(item.fields.join(' ')));
      assert.equal(result.status, 2, result.stderr || result.stdout);
      assert.match(result.stderr, /selection|field/i);
    });
  }

  await t.test('Keeper isGap=false predicate is enforced', () => {
    const keeperRows = validLiveRows();
    keeperRows.LeaderboardKeeperUserSettled = [
      { id: 'raw-a', user_id: USER_A, epochNumber: '8', isGap: false },
      { id: 'raw-b', user_id: USER_B, epochNumber: '8', isGap: true },
    ];
    const query = `query { LeaderboardKeeperUserSettled(where: { id: { _gt: "" }, isGap: { _eq: false } }, order_by: { id: asc }, limit: 1000) { id user_id epochNumber isGap } }`;
    const result = runLiveMockProbe(keeperRows, query);
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const payload = JSON.parse(result.stdout) as {
      data: { LeaderboardKeeperUserSettled: Array<{ isGap: boolean }> };
    };
    assert.deepEqual(
      payload.data.LeaderboardKeeperUserSettled.map(row => row.isGap),
      [false]
    );
  });
});
