/**
 * Constants for the Neverland Protocol indexer
 */

// Time constants
export const SECONDS_PER_DAY = 86400;
export const HOURS_PER_DAY = 24;
export const LEADERBOARD_START_BLOCK = 46264051;
export const DUST_LOCK_START_BLOCK = 39468872;
export const LP_V2_CUTOVER_BLOCK = 56436798;
export const LP_V2_CUTOVER_TIMESTAMP = 1771517877;

// Balancer AutoRange V3 USDC/DUST pool — active LP points source after the
// UniswapV2 cutover. LP points cut over from the V2 pair to Balancer AutoRange
// at this block/timestamp.
export const LP_BALANCER_AUTORANGE_CUTOVER_BLOCK = 78741015;
export const LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP = 1780444800;
// A settlement cursor entry is considered stale after this many seconds and is
// force-settled on the next clockwise sweep.
export const LP_BALANCER_STALE_SETTLEMENT_SECONDS = 1800;
// Upper bound on how many LP positions a single Balancer swap will settle, to
// keep per-event work bounded.
export const LP_BALANCER_MAX_SETTLEMENTS_PER_SWAP = 50;
export const BALANCER_AUTORANGE_V3_POOL_ADDRESS = '0x27da8a34579fbc99319af1c1a0f0d51065084576';
export const BALANCER_VAULT_ADDRESS = '0xba1333333333a1ba1108e8412f11850a5c319ba9';

// LP points bounce back from Balancer AutoRange to the UniswapV2 USDC/DUST pair
// at this block/timestamp: Balancer AutoRange stops accruing points and the V2
// pair resumes as the active LP points source.
export const LP_V2_RESUME_CUTOVER_BLOCK = 87190222;
export const LP_V2_RESUME_CUTOVER_TIMESTAMP = 1783827555;

// Override epoch 1 start time - set to a timestamp to ignore the EpochStart event for epoch 1
// and use this timestamp instead. Set to 0 to use the on-chain event.
export const EPOCH_1_START_TIME_OVERRIDE = 1767434400;
export const EPOCH_1_END_TIME_OVERRIDE = 1769983200;
// Deterministic start block for epoch 1 (set to 0 to use first event's block)
export const EPOCH_1_START_BLOCK_OVERRIDE = 46264051;

// Bootstrap LeaderboardConfig when epoch 1 is overridden (no events received)
// These values should match what the contracts would emit
export const BOOTSTRAP_CONFIG = {
  depositRateBps: 200n, // 2% per day in basis points
  borrowRateBps: 500n, // 5% per day in basis points
  vpRateBps: 2500n, // 25% per day in basis points
  lpRateBps: 2500n, // 25% per day in basis points
  supplyDailyBonus: 0,
  borrowDailyBonus: 0,
  repayDailyBonus: 0,
  withdrawDailyBonus: 0,
  cooldownSeconds: 0,
  minDailyBonusUsd: 0,
};

// Bootstrap VotingPowerMultiplier tiers when epoch 1 is overridden
// Format: [minVotingPower, multiplierBps]
export const BOOTSTRAP_VP_TIERS: Array<[bigint, bigint]> = [
  // Example: [0n, 10000n], // Tier 0: 0 VP = 1x multiplier (10000 bps)
  // [1000n * 10n**18n, 15000n], // Tier 1: 1000 VP = 1.5x multiplier
];

// Bootstrap NFT Partnerships (3 collections)
// Each entry: { collection, name, startTimestamp, endTimestamp (optional) }
export interface BootstrapNFTPartnership {
  collection: string;
  name: string;
  staticBoostBps?: bigint;
  startTimestamp: number;
  endTimestamp?: number;
}
export const BOOTSTRAP_NFT_PARTNERSHIPS: BootstrapNFTPartnership[] = [
  {
    collection: '0x818030837e8350ba63e64d7dc01a547fa73c8279',
    name: 'The 10k Squad',
    staticBoostBps: 2000n,
    startTimestamp: 0,
  },
  { collection: '0xfb5ba4061f5c50b1daa6c067bb2dfb0a8ebf6a8d', name: 'Overnads', startTimestamp: 0 },
];

// Bootstrap NFT Multiplier Config (firstBonus, decayRatio)
export const BOOTSTRAP_NFT_MULTIPLIER_CONFIG = {
  firstBonus: 1000n, // 10% bonus in basis points
  decayRatio: 9000n, // 90% decay ratio
};

// Bootstrap LP Pool Config
// The pool you're already tracking via HARDCODED_LP_POOL
export interface BootstrapLPPoolConfig {
  pool: string;
  positionManager: string;
  token0: string;
  token1: string;
  fee?: number;
  lpRateBps: bigint;
}
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

// Basis points
export const BASIS_POINTS = 10000n;
export const BASIS_POINTS_FLOAT = 10000;

// Fixed-point scale for points (1e18 precision)
export const POINTS_SCALE = 10n ** 18n;
export const POINTS_SCALE_FLOAT = 1e18;

// Helper: multiply a float by POINTS_SCALE and return BigInt
export function toScaledPoints(value: number): bigint {
  // Multiply by scale, then truncate to integer
  return BigInt(Math.floor(value * POINTS_SCALE_FLOAT));
}

// Helper: convert scaled BigInt points back to float (for display/comparison)
export function fromScaledPoints(scaled: bigint): number {
  return Number(scaled) / POINTS_SCALE_FLOAT;
}

// Default rates if config not set
export const DEFAULT_DEPOSIT_RATE_BPS = 100n; // 0.01 per day
export const DEFAULT_BORROW_RATE_BPS = 500n; // 0.05 per day

// Maximum multiplier cap (10x)
export const MAX_MULTIPLIER = 10;

// veNFT lock duration (365 days in seconds)
export const MAX_LOCK_TIME = 31536000n;

// Zero address
export const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
export const DUST_LOCK_ADDRESS = '0xbb4738d05ad1b3da57a4881bae62ce9bb1eeed6c';
export const NFT_PARTNERSHIP_REGISTRY_ADDRESS = '0xd936a70bd854a88c4b0d7fb21091ebc6209b13e2';

// Known gateway addresses for WMON/MON withdrawals
// These contracts should NOT accrue points - they are intermediaries
export const KNOWN_GATEWAYS = ['0x800409dbd7157813bb76501c30e04596cc478f25']; // WrappedTokenGatewayV3

export function isGatewayAddress(address: string): boolean {
  return KNOWN_GATEWAYS.includes(address.toLowerCase());
}

// Statically-configured NFT collections: these addresses are listed in
// config.yaml `networks[].contracts` with a hardcoded address and their own
// Transfer handler, so they are already indexed under their own contract name.
// They must NEVER be re-registered as the dynamic `PartnerNFT` contract. Envio
// keys dynamic registrations by (contractName, address) and does NOT dedupe
// across contract names, so a live `NFTPartnershipRegistry.PartnershipAdded` for
// one of these would dispatch each Transfer log to BOTH the static handler AND
// the PartnerNFT handler, double-applying +1/-1 balance deltas and corrupting
// nftCount/nftMultiplier. Keep this list in sync with the static NFT entries in
// config.yaml.
export const STATIC_NFT_COLLECTION_ADDRESSES = [
  '0x818030837e8350ba63e64d7dc01a547fa73c8279', // The10kSquad
  '0xfb5ba4061f5c50b1daa6c067bb2dfb0a8ebf6a8d', // Overnads
  '0xcabf3c04b90f4fe1b521fcaf4acb25d5df478e52', // LilStars
  '0xe20c4f8cacdb1854151f3e12144bdc919e608b9b', // RealNads
];

export function isStaticNftCollection(address: string): boolean {
  return STATIC_NFT_COLLECTION_ADDRESSES.includes(address.toLowerCase());
}

// Aave V3 Protocol Identifiers (bytes32)
// These are ASCII strings encoded as bytes32, used by PoolAddressesProvider
// to identify which type of proxy contract is being created in ProxyCreated events
export const POOL_ID = '0x504f4f4c00000000000000000000000000000000000000000000000000000000'; // keccak256("POOL")
export const POOL_CONFIGURATOR_ID =
  '0x504f4f4c5f434f4e464947555241544f52000000000000000000000000000000'; // keccak256("POOL_CONFIGURATOR")
export const POOL_ADMIN_ID = '0x504f4f4c5f41444d494e00000000000000000000000000000000000000000000'; // keccak256("POOL_ADMIN")
export const EMERGENCY_ADMIN_ID =
  '0x454d455247454e43595f41444d494e0000000000000000000000000000000000'; // keccak256("EMERGENCY_ADMIN")

// Treasury addresses (mints to these are protocol revenue, not user deposits)
export const TREASURY_ADDRESSES = [
  '0xb2289e329d2f85f1ed31adbb30ea345278f21bcf',
  '0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383',
  '0xbe85413851d195fc6341619cd68bfdc26a25b928',
  '0x5ba7fd868c40c16f7adfae6cf87121e13fc2f7a0',
  '0x8a020d92d6b119978582be4d3edfdc9f7b28bf31',
  '0x053d55f9b5af8694c503eb288a1b7e552f590710',
  '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c',
];

// Known token addresses
export const WMON_ADDRESS = '0x3bd359c1119da7da1d913d1c4d2b7c461115433a';
export const WBTC_ADDRESS = '0x0555e30da8f98308edb960aa94c0db47230d2b9c';
export const WETH_ADDRESS = '0xee8c0e9f1bffb4eb878d8f15f368a02a35481242';
export const USDC_ADDRESS = '0x754704bc059f8c67012fed69bc8a327a5aafb603';
export const USDT0_ADDRESS = '0xe7cd86e13ac4309349f30b3435a9d337750fc82d';
export const AUSD_ADDRESS = '0x00000000efe302beaa2b3e6e1b18d08d69a9012a';
export const EARNAUSD_ADDRESS = '0x103222f020e98bba0ad9809a011fdf8e6f067496';
export const SAUSD_ADDRESS = '0xd793c04b87386a6bb84ee61d98e0065fde7fda5e';
// PT-AUSD-8OCT2026: Pendle PT reserve listed in the isolated neverland-pendle-ausd pool.
export const PT_AUSD_8OCT2026_ADDRESS = '0x9fc74f8ed616b5baf52a170caa97d6d3898602d1';
export const GMON_ADDRESS = '0x8498312a6b3cbd158bf0c93abdcf29e6e4f55081';
export const SMON_ADDRESS = '0xa3227c5969757783154c60bf0bc1944180ed81b9';
export const SHMON_ADDRESS = '0x1b68626dca36c7fe922fd2d55e4f631d962de19c';
export const SHMON_UPPER_ADDRESS = '0x1ce060d47a0fd08b0869748fd7eccf151f4ec5d1';
export const LOAZND_ADDRESS = '0x9c82eb49b51f7dc61e22ff347931ca32adc6cd90';
export const CBBTC_ADDRESS = '0xd18b7ec58cdf4876f6afebd3ed1730e4ce10414b';

export interface TokenMetadata {
  name: string;
  symbol: string;
  decimals: number;
}

// Known token metadata for common tokens on Monad
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
  [PT_AUSD_8OCT2026_ADDRESS]: { name: 'PT AUSD 8OCT2026', symbol: 'PT-AUSD-8OCT2026', decimals: 6 },
};

export function getTokenMetadata(address: string): TokenMetadata | null {
  const addr = address.toLowerCase();
  return KNOWN_TOKENS[addr] ?? null;
}

export function normalizeAddress(address: string): string {
  return address.toLowerCase();
}
