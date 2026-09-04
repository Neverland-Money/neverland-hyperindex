import {
  AUSD_ADDRESS,
  AUSD_DECIMALS_FALLBACK,
  DUST_DECIMALS,
  USDC_ADDRESS,
  USDT0_ADDRESS,
  normalizeAddress,
} from '../helpers/constants';

import type { EvmOnEventContext as handlerContext } from 'envio';
export function isFungibleLPPoolConfig(config: { pool: string; positionManager: string }): boolean {
  return normalizeAddress(config.positionManager) === normalizeAddress(config.pool);
}

export function isStableUsdToken(token: string): boolean {
  const normalizedToken = normalizeAddress(token);
  return (
    normalizedToken === AUSD_ADDRESS ||
    normalizedToken === USDC_ADDRESS ||
    normalizedToken === USDT0_ADDRESS
  );
}

async function getTokenDecimals(
  context: handlerContext,
  tokenAddress: string,
  fallbackDecimals: number,
  timestamp?: number
): Promise<number> {
  void timestamp;
  const tokenId = normalizeAddress(tokenAddress);
  const tokenInfo = await context.TokenInfo.get(tokenId);
  if (tokenInfo?.decimals !== undefined && tokenInfo.decimals > 0) {
    return tokenInfo.decimals;
  }

  return tokenInfo?.decimals ?? fallbackDecimals;
}

export async function getLPPoolTokenDecimals(
  context: handlerContext,
  config: { token0: string; token1: string },
  timestamp?: number
): Promise<{ token0Decimals: number; token1Decimals: number }> {
  const token0Fallback = isStableUsdToken(config.token0) ? AUSD_DECIMALS_FALLBACK : DUST_DECIMALS;
  const token1Fallback = isStableUsdToken(config.token1) ? AUSD_DECIMALS_FALLBACK : DUST_DECIMALS;
  const token0Decimals = await getTokenDecimals(context, config.token0, token0Fallback, timestamp);
  const token1Decimals = await getTokenDecimals(context, config.token1, token1Fallback, timestamp);

  return { token0Decimals, token1Decimals };
}
