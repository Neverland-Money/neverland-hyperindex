/**
 * Viem utilities for contract reads via eth_call
 * Centralized RPC client configuration and common contract read operations
 */

import { createPublicClient, http, type Address, defineChain } from 'viem';

// Define Monad chain
export const monadChain = defineChain({
  id: 143,
  name: 'Monad',
  nativeCurrency: { name: 'Monad', symbol: 'MON', decimals: 18 },
  rpcUrls: {
    default: { http: ['https://rpc-mainnet.monadinfra.com'] },
  },
  blockExplorers: {
    default: { name: 'MonadVision', url: 'https://monadvision.com' },
  },
});

/**
 * Shared public client for contract reads
 *
 * IMPORTANT: Uses https://rpc-mainnet.monadinfra.com which provides:
 * - Historical state access for eth_call, eth_getBalance, eth_getCode, etc.
 * - Lookback depends on RPC provider's disk capacity (~40k blocks on 2TB SSD)
 * - Regular full nodes (rpc.monad.xyz) do NOT provide arbitrary historic state
 *
 * Due to Monad's high throughput (5000 tx/block @ 0.4s), historical state access
 * is limited. Use events/indexer for long-term historical data needs.
 */
export const publicClient = createPublicClient({
  chain: monadChain,
  transport: http(),
});

// Standard ERC20 ABI for common reads
export const ERC20_ABI = [
  {
    inputs: [{ name: 'account', type: 'address' }],
    name: 'balanceOf',
    outputs: [{ type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'name',
    outputs: [{ type: 'string' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'decimals',
    outputs: [{ type: 'uint8' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'symbol',
    outputs: [{ type: 'string' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

// Standard ERC721 ABI for NFT reads
export const ERC721_ABI = [
  {
    inputs: [{ name: 'owner', type: 'address' }],
    name: 'balanceOf',
    outputs: [{ type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ name: 'tokenId', type: 'uint256' }],
    name: 'ownerOf',
    outputs: [{ type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

export const NFT_PARTNERSHIP_REGISTRY_ABI = [
  {
    inputs: [],
    name: 'getActivePartnerships',
    outputs: [{ type: 'address[]' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

/**
 * Read ERC721 NFT balance for an address at a specific block
 *
 * @param nftContract - NFT contract address
 * @param ownerAddress - Owner address to check
 * @param blockNumber - Block number to query at (optional, defaults to latest)
 * @returns Balance if successful, null if historical state unavailable
 *
 * Note: Monad's historical RPC only maintains ~40k blocks of state.
 * If querying older blocks, this will return null and caller should
 * fall back to event-driven tracking.
 */
export async function readNFTBalance(
  nftContract: string,
  ownerAddress: string,
  blockNumber?: bigint
): Promise<bigint | null> {
  try {
    return (await publicClient.readContract({
      address: nftContract as Address,
      abi: ERC721_ABI,
      functionName: 'balanceOf',
      args: [ownerAddress as Address],
      blockNumber,
    })) as bigint;
  } catch {
    // Historical state not available - return null to signal fallback needed
    // console.warn(
    //   `Failed to read NFT balance for ${nftContract}:${ownerAddress} at block ${blockNumber}. ` +
    //     `Historical state may not be available. Error: ${error}`
    // );
    return null;
  }
}

export async function readActivePartnerships(
  registryAddress: string,
  blockNumber?: bigint
): Promise<string[] | null> {
  try {
    const params: { blockNumber?: bigint } = {};
    if (blockNumber !== undefined) {
      params.blockNumber = blockNumber;
    }

    return (await publicClient.readContract({
      address: registryAddress as Address,
      abi: NFT_PARTNERSHIP_REGISTRY_ABI,
      functionName: 'getActivePartnerships',
      ...params,
    })) as string[];
  } catch {
    // console.warn(
    //   `Failed to read active NFT partnerships at block ${blockNumber}. ` +
    //     `Historical state may not be available. Error: ${error}`
    // );
    return null;
  }
}

/**
 * Read ERC20 token balance for an address
 */
export async function readTokenBalance(
  tokenContract: string,
  ownerAddress: string
): Promise<bigint> {
  return (await publicClient.readContract({
    address: tokenContract as Address,
    abi: ERC20_ABI,
    functionName: 'balanceOf',
    args: [ownerAddress as Address],
  })) as bigint;
}

/**
 * Read ERC20 token decimals
 */
export async function readTokenDecimals(
  tokenContract: string,
  blockNumber?: bigint
): Promise<number> {
  const params: { blockNumber?: bigint } = {};
  if (blockNumber !== undefined) {
    params.blockNumber = blockNumber;
  }
  return (await publicClient.readContract({
    address: tokenContract as Address,
    abi: ERC20_ABI,
    functionName: 'decimals',
    ...params,
  })) as number;
}

/**
 * Read ERC20 token symbol
 */
export async function readTokenSymbol(
  tokenContract: string,
  blockNumber?: bigint
): Promise<string> {
  const params: { blockNumber?: bigint } = {};
  if (blockNumber !== undefined) {
    params.blockNumber = blockNumber;
  }
  return (await publicClient.readContract({
    address: tokenContract as Address,
    abi: ERC20_ABI,
    functionName: 'symbol',
    ...params,
  })) as string;
}

/**
 * Read ERC20 token name
 */
export async function readTokenName(tokenContract: string, blockNumber?: bigint): Promise<string> {
  const params: { blockNumber?: bigint } = {};
  if (blockNumber !== undefined) {
    params.blockNumber = blockNumber;
  }
  return (await publicClient.readContract({
    address: tokenContract as Address,
    abi: ERC20_ABI,
    functionName: 'name',
    ...params,
  })) as string;
}

export async function tryReadTokenMetadata(
  tokenContract: string,
  blockNumber?: bigint
): Promise<{ symbol?: string; name?: string; decimals?: number } | null> {
  const attempt = async (block?: bigint) => {
    let symbol: string | undefined;
    let name: string | undefined;
    let decimals: number | undefined;

    try {
      symbol = await readTokenSymbol(tokenContract, block);
    } catch {
      symbol = undefined;
    }

    try {
      name = await readTokenName(tokenContract, block);
    } catch {
      name = undefined;
    }

    try {
      decimals = await readTokenDecimals(tokenContract, block);
    } catch {
      decimals = undefined;
    }

    if (!symbol && !name && decimals === undefined) {
      return null;
    }

    return { symbol, name, decimals };
  };

  let metadata = await attempt(blockNumber);
  if (!metadata && blockNumber !== undefined) {
    metadata = await attempt(undefined);
  }

  return metadata;
}

/**
 * Generic contract read with custom ABI
 */
export async function readContract<T = unknown>(
  contractAddress: string,
  abi: unknown[],
  functionName: string,
  args: unknown[] = []
): Promise<T> {
  return (await publicClient.readContract({
    address: contractAddress as Address,
    abi,
    functionName,
    args,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any)) as T;
}

// Uniswap V3 NonfungiblePositionManager ABI for positions()
const POSITION_MANAGER_ABI = [
  {
    inputs: [{ name: 'owner', type: 'address' }],
    name: 'balanceOf',
    outputs: [{ name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { name: 'owner', type: 'address' },
      { name: 'index', type: 'uint256' },
    ],
    name: 'tokenOfOwnerByIndex',
    outputs: [{ name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ name: 'tokenId', type: 'uint256' }],
    name: 'positions',
    outputs: [
      { name: 'nonce', type: 'uint96' },
      { name: 'operator', type: 'address' },
      { name: 'token0', type: 'address' },
      { name: 'token1', type: 'address' },
      { name: 'fee', type: 'uint24' },
      { name: 'tickLower', type: 'int24' },
      { name: 'tickUpper', type: 'int24' },
      { name: 'liquidity', type: 'uint128' },
      { name: 'feeGrowthInside0LastX128', type: 'uint256' },
      { name: 'feeGrowthInside1LastX128', type: 'uint256' },
      { name: 'tokensOwed0', type: 'uint128' },
      { name: 'tokensOwed1', type: 'uint128' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

// Uniswap V3 pool ABI for slot0()
const UNISWAP_V3_POOL_ABI = [
  {
    inputs: [],
    name: 'slot0',
    outputs: [
      { name: 'sqrtPriceX96', type: 'uint160' },
      { name: 'tick', type: 'int24' },
      { name: 'observationIndex', type: 'uint16' },
      { name: 'observationCardinality', type: 'uint16' },
      { name: 'observationCardinalityNext', type: 'uint16' },
      { name: 'feeProtocol', type: 'uint8' },
      { name: 'unlocked', type: 'bool' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'fee',
    outputs: [{ name: '', type: 'uint24' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

export interface LPPositionData {
  token0: string;
  token1: string;
  fee: number;
  tickLower: number;
  tickUpper: number;
  liquidity: bigint;
}

type LoggerLike = {
  debug?: (message: string) => void;
  error?: (message: string) => void;
};

function formatError(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

/**
 * Read LP position data from NonfungiblePositionManager
 * Returns position details including tick range for in-range calculation
 */
export async function readLPPosition(
  positionManager: string,
  tokenId: bigint,
  blockNumber?: bigint,
  log?: LoggerLike
): Promise<LPPositionData | null> {
  try {
    const result = (await publicClient.readContract({
      address: positionManager as Address,
      abi: POSITION_MANAGER_ABI,
      functionName: 'positions',
      args: [tokenId],
      blockNumber,
    })) as readonly [
      bigint,
      string,
      string,
      string,
      number,
      number,
      number,
      bigint,
      bigint,
      bigint,
      bigint,
      bigint,
    ];

    return {
      token0: result[2].toLowerCase(),
      token1: result[3].toLowerCase(),
      fee: result[4],
      tickLower: result[5],
      tickUpper: result[6],
      liquidity: result[7],
    };
  } catch (error) {
    if (log?.error) {
      const blockLabel = blockNumber ? blockNumber.toString() : 'latest';
      log.error(
        `[lp] eth_call positions failed manager=${positionManager} tokenId=${tokenId.toString()} block=${blockLabel} error=${formatError(error)}`
      );
    }
    return null;
  }
}

export async function readLPBalance(
  positionManager: string,
  owner: string,
  blockNumber?: bigint,
  log?: LoggerLike
): Promise<bigint | null> {
  try {
    return (await publicClient.readContract({
      address: positionManager as Address,
      abi: POSITION_MANAGER_ABI,
      functionName: 'balanceOf',
      args: [owner as Address],
      blockNumber,
    })) as bigint;
  } catch (error) {
    if (log?.error) {
      const blockLabel = blockNumber ? blockNumber.toString() : 'latest';
      log.error(
        `[lp] eth_call balanceOf failed manager=${positionManager} owner=${owner} block=${blockLabel} error=${formatError(error)}`
      );
    }
    return null;
  }
}

export async function readLPTokenOfOwnerByIndex(
  positionManager: string,
  owner: string,
  index: bigint,
  blockNumber?: bigint,
  log?: LoggerLike
): Promise<bigint | null> {
  try {
    return (await publicClient.readContract({
      address: positionManager as Address,
      abi: POSITION_MANAGER_ABI,
      functionName: 'tokenOfOwnerByIndex',
      args: [owner as Address, index],
      blockNumber,
    })) as bigint;
  } catch (error) {
    if (log?.error) {
      const blockLabel = blockNumber ? blockNumber.toString() : 'latest';
      log.error(
        `[lp] eth_call tokenOfOwnerByIndex failed manager=${positionManager} owner=${owner} index=${index.toString()} block=${blockLabel} error=${formatError(error)}`
      );
    }
    return null;
  }
}

export async function readPoolSlot0(
  pool: string,
  blockNumber?: bigint,
  log?: LoggerLike
): Promise<{ sqrtPriceX96: bigint; tick: number } | null> {
  try {
    const result = (await publicClient.readContract({
      address: pool as Address,
      abi: UNISWAP_V3_POOL_ABI,
      functionName: 'slot0',
      blockNumber,
    })) as readonly [bigint, number, number, number, number, number, boolean];

    return {
      sqrtPriceX96: result[0],
      tick: result[1],
    };
  } catch (error) {
    if (log?.error) {
      const blockLabel = blockNumber ? blockNumber.toString() : 'latest';
      log.error(
        `[lp] eth_call slot0 failed pool=${pool} block=${blockLabel} error=${formatError(error)}`
      );
    }
    return null;
  }
}

export async function readPoolFee(
  pool: string,
  blockNumber?: bigint,
  log?: LoggerLike
): Promise<number | null> {
  try {
    const result = (await publicClient.readContract({
      address: pool as Address,
      abi: UNISWAP_V3_POOL_ABI,
      functionName: 'fee',
      blockNumber,
    })) as bigint | number;
    return typeof result === 'bigint' ? Number(result) : result;
  } catch (error) {
    if (log?.error) {
      const blockLabel = blockNumber ? blockNumber.toString() : 'latest';
      log.error(
        `[lp] eth_call fee failed pool=${pool} block=${blockLabel} error=${formatError(error)}`
      );
    }
    return null;
  }
}
