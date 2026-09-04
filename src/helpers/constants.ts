/**
 * Every manual figure the indexer is fed: values a human chose rather than ones the chain
 * reports, covering addresses, start blocks, LP-era cutovers, epoch overrides, bootstrap seeds,
 * rates, caps, tick bounds, pool fees, and the string literals handlers compare against. A wrong
 * value here is a wrong index, and changing one generally means a resync. Not here: math
 * primitives that belong with their math (`RAY`, `WAD`, `Q96`, `Q192`, `LP_GROWTH_Q128`,
 * `MAX_TICK`) and ABIs.
 */

/*//////////////////////////////////////////////////////////////
                              TIME
//////////////////////////////////////////////////////////////*/

export const SECONDS_PER_DAY = 86400;
export const HOURS_PER_DAY = 24;

/*//////////////////////////////////////////////////////////////
                    START BLOCKS & CUTOVERS
//////////////////////////////////////////////////////////////*/

export const LEADERBOARD_START_BLOCK = 46264051;
export const DUST_LOCK_START_BLOCK = 39468872;

/** LP points cut over from the UniswapV3 AUSD/DUST pool to the UniswapV2 USDC/DUST pair. */
export const LP_V2_CUTOVER_BLOCK = 56436798;
export const LP_V2_CUTOVER_TIMESTAMP = 1771517877;

/** LP points cut over from the UniswapV2 pair to Balancer AutoRange V3 USDC/DUST. */
export const LP_BALANCER_AUTORANGE_CUTOVER_BLOCK = 78741015;
export const LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP = 1780444800;

/** LP points bounce back from Balancer AutoRange to the UniswapV2 USDC/DUST pair. */
export const LP_V2_RESUME_CUTOVER_BLOCK = 87190222;
export const LP_V2_RESUME_CUTOVER_TIMESTAMP = 1783827555;

/**
 * Before this block FlashLoanLogic split the premium: `percentMul(flashLoanPremiumToProtocol)`
 * to `accruedToTreasury`, the rest to LPs via `cumulateToLiquidityIndex`. From here the whole
 * premium accrues to the treasury with no liquidityIndex bump, and
 * `flashLoanPremiumToProtocol` survives for ABI and storage stability only. Upgrade tx
 * 0x62ee8a68a260bcfdd57c9c96ab5fd17ad90573f24fc21557ba033e452a943efc, 2026-07-04T21:21:47Z,
 * PoolUpdated 0xe3b56aad -> 0x507c53de.
 */
export const FLASH_LOAN_PREMIUM_TO_TREASURY_BLOCK = 85624557;

/*//////////////////////////////////////////////////////////////
                        EPOCH OVERRIDES
//////////////////////////////////////////////////////////////*/

export type EpochDatesOverride = {
  startTime: number;
  endTime: number;
  /** Bootstrapped epochs only, which have no event to take a block from; omit to use the first. */
  startBlock?: number;
  /** Seeds the leaderboard from nothing; disabled by ENVIO_DISABLE_BOOTSTRAP. */
  bootstrap?: boolean;
};

/** Named so the bootstrap seeding below can read its fields without a fallback. */
const EPOCH_1_DATES = {
  startTime: 1767434400,
  endTime: 1769983200,
  startBlock: 46264051,
  bootstrap: true,
} satisfies EpochDatesOverride;

/**
 * A listed epoch takes its dates from here and its EpochStart/EpochEnd payloads are ignored, so
 * a correction is permanent and no later on-chain end can move the tide. Epoch 1 predates
 * EpochManager and has no events at all, so its entry also bootstraps the leaderboard (see
 * bootstrapLeaderboardIfNeeded; ENVIO_DISABLE_BOOTSTRAP turns that off). Epoch 9 went on-chain
 * with its end timestamp in the start field, which EpochManager cannot rewrite, so its real
 * dates live here.
 */
export const EPOCH_DATES_OVERRIDES: Record<string, EpochDatesOverride> = {
  '1': EPOCH_1_DATES,
  '9': { startTime: 1787893200, endTime: 1790442000 },
};

/**
 * VP accrual weights a token by the slice of the window its holder owned it, from here on. Set
 * to epoch 9's start, the first tide it can affect: tides 1-8 are settled and paid, so they keep
 * the semantics they were scored under.
 */
export const VP_OWNERSHIP_WEIGHTING_FROM = EPOCH_DATES_OVERRIDES['9'].startTime;

export function getEpochDatesOverride(epochNumber: bigint | number): EpochDatesOverride | null {
  const override = EPOCH_DATES_OVERRIDES[epochNumber.toString()];
  if (!override) return null;
  if (override.bootstrap && process.env.ENVIO_DISABLE_BOOTSTRAP === 'true') return null;
  return override;
}

/** Bootstrap seeding reads these directly; derived above to keep one source of truth. */
export const EPOCH_1_START_TIME_OVERRIDE = EPOCH_1_DATES.startTime;
export const EPOCH_1_END_TIME_OVERRIDE = EPOCH_1_DATES.endTime;
export const EPOCH_1_START_BLOCK_OVERRIDE = EPOCH_1_DATES.startBlock;

/*//////////////////////////////////////////////////////////////
                           BOOTSTRAP
//////////////////////////////////////////////////////////////*/

/** Seeds LeaderboardConfig for the bootstrapped epoch 1; must match what the contracts emit. */
export const BOOTSTRAP_CONFIG = {
  depositRateBps: 200n, // 2%/day
  borrowRateBps: 500n, // 5%/day
  vpRateBps: 2500n, // 25%/day
  lpRateBps: 2500n, // 25%/day
  supplyDailyBonus: 0,
  borrowDailyBonus: 0,
  repayDailyBonus: 0,
  withdrawDailyBonus: 0,
  cooldownSeconds: 0,
  minDailyBonusUsd: 0,
};

/** VotingPowerMultiplier tiers for bootstrapped epoch 1, as [minVotingPower, multiplierBps]. */
export const BOOTSTRAP_VP_TIERS: Array<[bigint, bigint]> = [
  /* Example: [0n, 10000n] is 0 VP at 1x; [1000n * 10n ** 18n, 15000n] is 1000 VP at 1.5x. */
];

export interface BootstrapNFTPartnership {
  collection: string;
  name: string;
  staticBoostBps?: bigint;
  startTimestamp: number;
  endTimestamp?: number;
}

/** Partner collections seeded for bootstrapped epoch 1. */
export const BOOTSTRAP_NFT_PARTNERSHIPS: BootstrapNFTPartnership[] = [
  {
    collection: '0x818030837e8350ba63e64d7dc01a547fa73c8279',
    name: 'The 10k Squad',
    staticBoostBps: 2000n,
    startTimestamp: 0,
  },
  { collection: '0xfb5ba4061f5c50b1daa6c067bb2dfb0a8ebf6a8d', name: 'Overnads', startTimestamp: 0 },
];

/** NFT multiplier config for bootstrapped epoch 1. */
export const BOOTSTRAP_NFT_MULTIPLIER_CONFIG = {
  firstBonus: 1000n, // 10%
  decayRatio: 9000n, // 90%
};

export interface BootstrapLPPoolConfig {
  pool: string;
  positionManager: string;
  token0: string;
  token1: string;
  fee?: number;
  lpRateBps: bigint;
}

/** LP pool seeded for bootstrapped epoch 1. */
export const BOOTSTRAP_LP_POOL_CONFIGS: BootstrapLPPoolConfig[] = [
  {
    pool: '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f',
    positionManager: '0x7197e214c0b767cfb76fb734ab638e2c192f4e53',
    token0: '0x00000000efe302beaa2b3e6e1b18d08d69a9012a', // AUSD
    token1: '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c', // DUST
    fee: 10000,
    lpRateBps: 2500n,
  },
];

/*//////////////////////////////////////////////////////////////
                         POINTS & RATES
//////////////////////////////////////////////////////////////*/

export const BASIS_POINTS = 10000n;
export const BASIS_POINTS_FLOAT = 10000;

/** Fixed-point scale for points. */
export const POINTS_SCALE = 10n ** 18n;
export const POINTS_SCALE_FLOAT = 1e18;

/** Scales a float into fixed-point points, rounding down. */
export function toScaledPoints(value: number): bigint {
  return BigInt(Math.floor(value * POINTS_SCALE_FLOAT));
}

/** Converts fixed-point points back to a float, for display and comparison only. */
export function fromScaledPoints(scaled: bigint): number {
  return Number(scaled) / POINTS_SCALE_FLOAT;
}

/** Rates used when the leaderboard config sets none. */
export const DEFAULT_DEPOSIT_RATE_BPS = 100n; // 1%/day
export const DEFAULT_BORROW_RATE_BPS = 500n; // 5%/day

/*//////////////////////////////////////////////////////////////
                        CAPS & COOLDOWNS
//////////////////////////////////////////////////////////////*/

/** Ceiling on a user's stacked multiplier, as a plain integer factor (10 = 10x). */
export const MAX_MULTIPLIER = 10;

/** veNFT lock ceiling: 365 days, in seconds. */
export const MAX_LOCK_TIME = 31536000n;

/** Ceilings on the VP tier table and on the product of every multiplier a user can stack. */
export const MAX_VP_TIERS = 20;
export const MAX_VP_MULTIPLIER = 50000n;
export const MAX_COMBINED_MULTIPLIER = 100000n;

/** Cooldown between point-earning actions when the leaderboard config sets none. */
export const DEFAULT_COOLDOWN_SECONDS = 3600;

/** Scheduled Tide transitions one event may apply, so a long gap cannot fan out in one pass. */
export const MAX_SCHEDULED_TRANSITIONS = 5;

/** veDUST lock checkpoints are week-aligned. */
export const SECONDS_PER_WEEK = 7 * 24 * 60 * 60;

/*//////////////////////////////////////////////////////////////
                       POINT-ACCRUAL BLACKLIST
//////////////////////////////////////////////////////////////*/

/**
 * Tide 9 start. Blacklisted addresses accrue nothing from here on; Tides 1-8 keep the
 * values they were scored and paid under.
 *
 * Timestamp only, deliberately: `EPOCH_DATES_OVERRIDES['9']` defines the boundary by
 * time, and every accrual site already has `event.block.timestamp`. Pinning a block as
 * well would create two signals that can disagree at the boundary under sub-second
 * blocks -- the failure applyStaticLPPoolCutover exists to avoid.
 */
export const POINT_ACCRUAL_BLACKLIST_FROM = EPOCH_DATES_OVERRIDES['9'].startTime;

/**
 * Addresses barred from accruing points from the start of Tide 9.
 *
 * Two sources, deduplicated: the tide-draw blacklist (14) and the Neverland
 * Foundation multisigs (12). The union is 25.
 *
 * Kept lowercase so membership is a plain Set lookup after normalizeAddress.
 */
export const POINT_ACCRUAL_BLACKLIST: ReadonlySet<string> = new Set([
  '0x82c370ba90e38ef6acd8b1b078d34fd86fc6bac9',
  '0x8d5c2df3eef09088fcccf3376d8ecd0dd505f642',
  '0x4e8aaecce10ad9394e96fe5f2bd4e587a7b04298',
  '0xdb39a9d4a1f1b4e93a5684d602207628ad60613c',
  '0x8959f4e6ed1f4567a464959793d5f8f6f33c1c8b',
  '0xb3b850ac62b89fe9f4efb652b516108a8aeb8848',
  '0x08139339dd9a480ceb84d9c7cce48be436db20b3',
  '0x5e073494678fb7fa4a05bb17d45941dd9dc469c1',
  '0x29d2075e5151b1a6863bdc40ea86bd5e8afd1705',
  '0xd45d54ad7ae6d5dedb0de7b283fe0b4e2ba40217',
  '0xd786f7569c39a9f64e6a54eb77db21364e90f279',
  '0x98a297e6424787e57af119949d7e00b721f832bb',
  '0x22139a346b6312eb0a9812c67cfce4a694676d59',
  '0x909b176220b7e782c0f3ceccab4b19d2c433c6bb',
  '0x49a18e0ffeb2a1254922675a854a0818b46446e2',
  '0x178d5f48a27f728e24e7d530a7c5c901778aade7',
  '0x8d3e4d6188d207641e3d8f9c08e43956d4daa66a',
  '0x6968d8e587824cd9a93b2ba515029b7e2085ef04',
  '0x430a7885d2dca795333ef6cbf00ce64ba0240411',
  '0x57976e192c45461f5958045a0bc57102e90440ed',
  '0xb83a6637c87e6a7192b3ada845c0745f815e9006',
  '0x51b1ac469dee0e8b3f9df3741fc222d33a375b8f',
  '0x06fe1d4f4b7ca7d3d1fb3164968f0f7383d02e0e',
  '0x6bb849d8d8d58d95323504444779d8e5cdaa4026',
  '0xe72df2dde84880dd706c5976e92ed34bb586a38f',
]);

// Whether `address` is barred from accruing points at `timestamp`.
export function isPointAccrualBlacklisted(address: string, timestamp: number): boolean {
  return timestamp >= POINT_ACCRUAL_BLACKLIST_FROM && POINT_ACCRUAL_BLACKLIST.has(address);
}

/*//////////////////////////////////////////////////////////////
                       PROTOCOL ADDRESSES
//////////////////////////////////////////////////////////////*/

/*
 * No treasury address list here on purpose: AToken.Initialized reports each aToken's treasury and
 * mintToTreasury emits Mint(caller = the Pool), so ATokenTreasury and the caller check in the
 * AToken.Mint handler recognize a treasury mint from the chain. A hardcoded list silently went
 * stale when the wallet changed, and adding the new address flipped the handler into a branch
 * that counted revenue twice.
 */

export const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
export const DUST_LOCK_ADDRESS = '0xbb4738d05ad1b3da57a4881bae62ce9bb1eeed6c';
export const BALANCER_AUTORANGE_V3_POOL_ADDRESS = '0x27da8a34579fbc99319af1c1a0f0d51065084576';
export const BALANCER_VAULT_ADDRESS = '0xba1333333333a1ba1108e8412f11850a5c319ba9';
export const NFT_PARTNERSHIP_REGISTRY_ADDRESS = '0xd936a70bd854a88c4b0d7fb21091ebc6209b13e2';

/** WMON/MON withdrawal gateways: intermediaries, so they never accrue points. */
export const KNOWN_GATEWAYS = ['0x800409dbd7157813bb76501c30e04596cc478f25']; // WrappedTokenGatewayV3

export function isGatewayAddress(address: string): boolean {
  return KNOWN_GATEWAYS.includes(address.toLowerCase());
}

/**
 * Listed in config.yaml `chains[].contracts` with their own Transfer handler, so they must NEVER
 * also be registered as the dynamic `PartnerNFT`: Envio keys registrations by (contractName,
 * address) and does NOT dedupe across contract names, so a live `PartnershipAdded` for one of
 * these would dispatch each Transfer to both handlers, double-applying the +1/-1 balance delta
 * and corrupting nftCount/nftMultiplier. Keep in sync with the static NFT entries in config.yaml.
 */
export const STATIC_NFT_COLLECTION_ADDRESSES = [
  '0x818030837e8350ba63e64d7dc01a547fa73c8279', // The10kSquad
  '0xfb5ba4061f5c50b1daa6c067bb2dfb0a8ebf6a8d', // Overnads
  '0xcabf3c04b90f4fe1b521fcaf4acb25d5df478e52', // LilStars
  '0xe20c4f8cacdb1854151f3e12144bdc919e608b9b', // RealNads
];

export function isStaticNftCollection(address: string): boolean {
  return STATIC_NFT_COLLECTION_ADDRESSES.includes(address.toLowerCase());
}

/*//////////////////////////////////////////////////////////////
                       AAVE PROTOCOL IDS
//////////////////////////////////////////////////////////////*/

/** ASCII encoded as bytes32: proxy kinds in ProxyCreated, and PoolAddressesProvider roles. */
export const POOL_ID = '0x504f4f4c00000000000000000000000000000000000000000000000000000000'; // bytes32("POOL")
export const POOL_CONFIGURATOR_ID =
  '0x504f4f4c5f434f4e464947555241544f52000000000000000000000000000000'; // bytes32("POOL_CONFIGURATOR")
export const POOL_ADMIN_ID = '0x504f4f4c5f41444d494e00000000000000000000000000000000000000000000'; // bytes32("POOL_ADMIN")
export const EMERGENCY_ADMIN_ID =
  '0x454d455247454e43595f41444d494e0000000000000000000000000000000000'; // bytes32("EMERGENCY_ADMIN")

/*//////////////////////////////////////////////////////////////
                        TOKEN ADDRESSES
//////////////////////////////////////////////////////////////*/

// Canonical Market
export const WMON_ADDRESS = '0x3bd359c1119da7da1d913d1c4d2b7c461115433a';
export const WBTC_ADDRESS = '0x0555e30da8f98308edb960aa94c0db47230d2b9c';
export const WETH_ADDRESS = '0xee8c0e9f1bffb4eb878d8f15f368a02a35481242';
export const USDC_ADDRESS = '0x754704bc059f8c67012fed69bc8a327a5aafb603';
export const USDT0_ADDRESS = '0xe7cd86e13ac4309349f30b3435a9d337750fc82d';
export const AUSD_ADDRESS = '0x00000000efe302beaa2b3e6e1b18d08d69a9012a';
export const EARNAUSD_ADDRESS = '0x103222f020e98bba0ad9809a011fdf8e6f067496';
export const GMON_ADDRESS = '0x8498312a6b3cbd158bf0c93abdcf29e6e4f55081';
export const SMON_ADDRESS = '0xa3227c5969757783154c60bf0bc1944180ed81b9';
export const SHMON_ADDRESS = '0x1b68626dca36c7fe922fd2d55e4f631d962de19c';
export const SHMON_UPPER_ADDRESS = '0x1ce060d47a0fd08b0869748fd7eccf151f4ec5d1';
export const LOAZND_ADDRESS = '0x9c82eb49b51f7dc61e22ff347931ca32adc6cd90';
export const CBBTC_ADDRESS = '0xd18b7ec58cdf4876f6afebd3ed1730e4ce10414b';
export const XAUT0_ADDRESS = '0x01bff41798a0bcf287b996046ca68b395dbc1071';
export const DUST_TOKEN_ADDRESS = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';

/** Decimal fallbacks used when a pair token has no TokenInfo row yet. */
export const DUST_DECIMALS = 18;
export const AUSD_DECIMALS_FALLBACK = 6;

// Isolated Markets
export const PT_AUSD_8OCT2026_ADDRESS = '0x9fc74f8ed616b5baf52a170caa97d6d3898602d1';
export const PT_SHMON_18MAR2027_ADDRESS = '0xa7deac306a4520f4f2f94d150ca2fbd13080b607';

/*//////////////////////////////////////////////////////////////
                          KNOWN TOKENS
//////////////////////////////////////////////////////////////*/

/** Curated display metadata for a listed reserve. */
export interface TokenMetadata {
  name: string;
  symbol: string;
  decimals: number;
}

/**
 * Authoritative: a listed asset takes name, symbol and decimals from here and whatever
 * `AToken.Initialized` reports is ignored, so correcting a reserve means editing this entry. An
 * absent asset is derived from that event instead.
 */
const KNOWN_TOKENS: Record<string, TokenMetadata> = {
  [WMON_ADDRESS]: { name: 'Wrapped MON', symbol: 'WMON', decimals: 18 },
  [SMON_ADDRESS]: { name: 'Kintsu Staked Monad', symbol: 'sMON', decimals: 18 },
  [GMON_ADDRESS]: { name: 'gMON', symbol: 'gMON', decimals: 18 },
  [SHMON_ADDRESS]: { name: 'ShMonad', symbol: 'shMON', decimals: 18 },
  [SHMON_UPPER_ADDRESS]: { name: 'ShMonad', symbol: 'shMON', decimals: 18 },
  [LOAZND_ADDRESS]: { name: 'Locked AZND', symbol: 'loAZND', decimals: 18 },
  [CBBTC_ADDRESS]: { name: 'Coinbase Wrapped BTC', symbol: 'cbBTC', decimals: 8 },
  [WETH_ADDRESS]: { name: 'Wrapped Ether', symbol: 'WETH', decimals: 18 },
  [AUSD_ADDRESS]: { name: 'AUSD', symbol: 'AUSD', decimals: 6 },
  [USDC_ADDRESS]: { name: 'USDC', symbol: 'USDC', decimals: 6 },
  [USDT0_ADDRESS]: { name: 'USDT0', symbol: 'USDT0', decimals: 6 },
  [EARNAUSD_ADDRESS]: { name: 'earnAUSD', symbol: 'earnAUSD', decimals: 6 },
  [WBTC_ADDRESS]: { name: 'Wrapped BTC', symbol: 'WBTC', decimals: 8 },
  [XAUT0_ADDRESS]: { name: 'XAUt0', symbol: 'XAUt0', decimals: 6 },
  // Isolated Markets
  [PT_AUSD_8OCT2026_ADDRESS]: { name: 'PT AUSD 8OCT2026', symbol: 'PT-AUSD-8OCT2026', decimals: 6 },
  [PT_SHMON_18MAR2027_ADDRESS]: {
    name: 'PT ShMonad 18MAR2027',
    symbol: 'PT-shMON-18MAR2027',
    decimals: 18,
  },
};

export function getTokenMetadata(address: string): TokenMetadata | null {
  const addr = address.toLowerCase();
  return KNOWN_TOKENS[addr] ?? null;
}

/**
 * Recovers the underlying reserve symbol from an AToken's own on-chain metadata. The deployer
 * (`add-reserve-multisig.ts` in neverland-pool-operations) builds both strings from one
 * `reserveSymbol`: name = `${ATokenNamePrefix} ${reserveSymbol}`, symbol =
 * `${SymbolPrefix}${reserveSymbol}`. Both prefixes vary per market ('Neverland Interest
 * Bearing'/'n' canonical, 'Neverland Pendle'/'np' for the Pendle markets), so a hardcoded
 * canonical prefix leaves isolated-market ATokens unstripped; every Neverland `SymbolPrefix` is
 * lowercase while `reserveSymbol` is uppercase, so dropping the leading lowercase run recovers it
 * without knowing the market. The name is the fallback for a non-lowercase prefix ('Testnet'/
 * 'Test'), whose last whitespace-delimited word is `reserveSymbol` by construction. The deployer
 * uppercases the symbol (`loAZND` -> `nLOAZND`), so this yields 'LOAZND', not the display casing
 * that KNOWN_TOKENS supplies; it keeps a reserve missing from that table labeled rather than
 * prefixed.
 */
export function deriveReserveSymbolFromAToken(aTokenName: string, aTokenSymbol: string): string {
  const stripped = aTokenSymbol.replace(/^[a-z]+/, '');
  if (stripped && stripped !== aTokenSymbol) {
    return stripped;
  }

  /* No lowercase prefix to drop; the name's last word is the reserve symbol. */
  const nameParts = aTokenName.trim().split(/\s+/);
  if (nameParts.length > 1) {
    return nameParts[nameParts.length - 1];
  }

  return aTokenSymbol || aTokenName.trim();
}

/*//////////////////////////////////////////////////////////////
                            LP ERAS
//////////////////////////////////////////////////////////////*/

/**
 * UniswapV3 (AUSD/DUST) -> UniswapV2 pair (USDC/DUST) -> Balancer AutoRange V3 (USDC/DUST) ->
 * back to the UniswapV2 pair. All four contracts stay registered in config.yaml;
 * `applyStaticLPPoolCutover` in lp.ts picks the era accruing for a block from the
 * `LP_*_CUTOVER_BLOCK` / `_TIMESTAMP` pairs above.
 */

/** Era 1: UniswapV3 AUSD/DUST. */
export const LEGACY_V3_LP_POOL = normalizeAddress('0xd15965968fe8bf2babbe39b2fc5de1ab6749141f');
export const LEGACY_V3_LP_POSITION_MANAGER = normalizeAddress(
  '0x7197e214c0b767cfb76fb734ab638e2c192f4e53'
);
export const LEGACY_V3_LP_TOKEN0 = AUSD_ADDRESS;
export const LEGACY_V3_LP_TOKEN1 = DUST_TOKEN_ADDRESS;
export const LEGACY_V3_LP_FEE = 10000;
export const LEGACY_V3_LP_START_BLOCK = 41231451n;

/**
 * Eras 2 and 4: the UniswapV2 USDC/DUST pair. Fungible, so its position manager is the pair
 * itself and its synthetic positions span the full tick range.
 */
export const V2_LP_POOL = normalizeAddress('0x86dbf00485871c901c5129bd525348db96c2eb2d');
export const V2_LP_POSITION_MANAGER = V2_LP_POOL;
export const V2_LP_TOKEN0 = USDC_ADDRESS;
export const V2_LP_TOKEN1 = DUST_TOKEN_ADDRESS;
export const V2_LP_FEE = 3000;
export const V2_TICK_LOWER = -887272;
export const V2_TICK_UPPER = 887272;

/** Era 3: Balancer AutoRange V3 USDC/DUST. Also fungible. */
export const BALANCER_AUTORANGE_V3_POOL = normalizeAddress(BALANCER_AUTORANGE_V3_POOL_ADDRESS);
export const BALANCER_AUTORANGE_V3_TOKEN0 = USDC_ADDRESS;
export const BALANCER_AUTORANGE_V3_TOKEN1 = DUST_TOKEN_ADDRESS;
export const BALANCER_AUTORANGE_V3_FEE = 10000;

export type LPStaticTransitionRecord = {
  id: string;
  outgoingPool: string;
  incomingPool: string;
  blockNumber: bigint;
  timestamp: number;
};

export const LEGACY_V3_TO_V2_TRANSITION = {
  id: 'legacy-v3-to-v2',
  outgoingPool: LEGACY_V3_LP_POOL,
  incomingPool: V2_LP_POOL,
  blockNumber: BigInt(LP_V2_CUTOVER_BLOCK),
  timestamp: LP_V2_CUTOVER_TIMESTAMP,
} satisfies LPStaticTransitionRecord;

export const V2_TO_BALANCER_TRANSITION = {
  id: 'v2-to-balancer-autorange',
  outgoingPool: V2_LP_POOL,
  incomingPool: BALANCER_AUTORANGE_V3_POOL,
  blockNumber: BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK),
  timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
} satisfies LPStaticTransitionRecord;

export const BALANCER_TO_V2_RESUME_TRANSITION = {
  id: 'balancer-to-v2-resume',
  outgoingPool: BALANCER_AUTORANGE_V3_POOL,
  incomingPool: V2_LP_POOL,
  blockNumber: BigInt(LP_V2_RESUME_CUTOVER_BLOCK),
  timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
} satisfies LPStaticTransitionRecord;

/*//////////////////////////////////////////////////////////////
                        LP FEE & VOLUME
//////////////////////////////////////////////////////////////*/

/** Uniswap encodes pool fees in hundredths of a bip, so a 3000 fee is 0.30%. */
export const FEE_UNITS_DENOMINATOR = 1_000_000n;

/** Swap volume is bucketed hourly and fee APR is annualized over a 24-bucket window. */
export const VOLUME_BUCKET_SECONDS = 3600;
export const VOLUME_WINDOW_HOURS = 24;
export const DAYS_PER_YEAR = 365n;

/*//////////////////////////////////////////////////////////////
                      LEADERBOARD HISTORY
//////////////////////////////////////////////////////////////*/

/** Ring size for per-user history buckets, and the epoch reserved for all-time aggregates. */
export const MAX_BUCKETS = 120;
export const ALL_TIME_EPOCH_NUMBER = 0n;

/*//////////////////////////////////////////////////////////////
                    SHOP & SPECIAL EDITIONS
//////////////////////////////////////////////////////////////*/

/** Item categories emitted by NeverlandProfileItemsSeller. */
export const CATEGORY_PERMANENT = 'Permanent';
export const CATEGORY_CONSUMABLE = 'Consumable';

/** Two-step ownership event names shared by the shop handlers. */
export const OWNERSHIP_TRANSFER_STARTED = 'OwnershipTransferStarted';
export const OWNERSHIP_TRANSFERRED = 'OwnershipTransferred';

/** Ledger action names for special-edition item movements. */
export const SPECIAL_EDITION_TRANSFER_OUT = 'SPECIAL_EDITION_TRANSFER_OUT';
export const SPECIAL_EDITION_TRANSFER_IN = 'SPECIAL_EDITION_TRANSFER_IN';

/*//////////////////////////////////////////////////////////////
                            HELPERS
//////////////////////////////////////////////////////////////*/

/**
 * Lowercases an address and brands it as envio's `Address` (`0x${string}`), which is what
 * `context.chain.<C>.add` and event `where` filters accept in v3. The single cast lives here
 * rather than at each of the ~430 call sites; entity id fields stay plain `string`, which
 * accepts the branded type without further widening.
 */
export function normalizeAddress(address: string): `0x${string}` {
  return address.toLowerCase() as `0x${string}`;
}
