import { publicClient } from '../helpers/viem';
import { NFT_PARTNERSHIP_REGISTRY_ADDRESS } from '../helpers/constants';

export const VIEM_ERROR_ADDRESS = '0x0000000000000000000000000000000000000bad';
export const VIEM_FALLBACK_ADDRESS = '0x0000000000000000000000000000000000000beef';
export const VIEM_PARTIAL_ADDRESS = '0x0000000000000000000000000000000000000caf';
export const VIEM_ATOKEN_ADDRESS = '0x0000000000000000000000000000000000000a70';
export const VIEM_NAME_ONLY_ADDRESS = '0x0000000000000000000000000000000000000a71';
export const VIEM_EMPTY_ATOKEN_ADDRESS = '0x0000000000000000000000000000000000000a72';
export const VIEM_ZERO_BALANCE_ADDRESS = '0x0000000000000000000000000000000000000a73';
export const VIEM_SECOND_NFT_ADDRESS = '0x0000000000000000000000000000000000000a74';
export const VIEM_NO_NFT_ADDRESS = '0x0000000000000000000000000000000000000a75';

let installed = false;
let activePartnershipsOverride: string[] | null | undefined;
let lpPositionOverride:
  | readonly [
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
    ]
  | null
  | undefined;
const lpBalanceOverrides = new Map<string, bigint | null>();
const lpTokenOverrides = new Map<string, bigint[] | null>();

type ReadContractParams = {
  address: string;
  functionName: string;
  blockNumber?: bigint;
  args?: readonly unknown[];
};

export function setActivePartnershipsOverride(value: string[] | null | undefined) {
  activePartnershipsOverride = value;
}

export function setLPPositionOverride(
  value:
    | readonly [
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
      ]
    | null
    | undefined
) {
  lpPositionOverride = value;
}

export function setLPBalanceOverride(
  positionManager: string,
  owner: string,
  value: bigint | null | undefined
) {
  const key = `${positionManager.toLowerCase()}:${owner.toLowerCase()}`;
  if (value === undefined) {
    lpBalanceOverrides.delete(key);
    return;
  }
  lpBalanceOverrides.set(key, value);
}

export function setLPTokensOverride(
  positionManager: string,
  owner: string,
  value: bigint[] | null | undefined
) {
  const key = `${positionManager.toLowerCase()}:${owner.toLowerCase()}`;
  if (value === undefined) {
    lpTokenOverrides.delete(key);
    return;
  }
  lpTokenOverrides.set(key, value);
}

export function installViemMock() {
  if (installed) return;
  installed = true;

  publicClient.readContract = async (params: ReadContractParams) => {
    if (params.address === VIEM_ERROR_ADDRESS) {
      throw new Error('boom');
    }
    if (params.address === VIEM_FALLBACK_ADDRESS && params.blockNumber !== undefined) {
      throw new Error('no historical state');
    }
    if (params.address === VIEM_PARTIAL_ADDRESS) {
      if (params.functionName === 'symbol') return 'PART';
      if (params.functionName === 'balanceOf') return 123n;
      throw new Error('partial metadata');
    }
    if (params.address === VIEM_ZERO_BALANCE_ADDRESS) {
      if (params.functionName === 'balanceOf') return 0n;
      throw new Error('zero balance');
    }
    if (params.address === VIEM_SECOND_NFT_ADDRESS) {
      if (params.functionName === 'balanceOf') return 123n;
      throw new Error('secondary balance');
    }
    if (params.address === VIEM_NO_NFT_ADDRESS) {
      if (params.functionName === 'balanceOf') return 0n;
      throw new Error('no nft');
    }
    if (params.address === VIEM_ATOKEN_ADDRESS) {
      if (params.functionName === 'symbol') return 'nABC';
      if (params.functionName === 'name') return 'Neverland Interest Bearing ABC';
      if (params.functionName === 'decimals') return 6;
    }
    if (params.address === VIEM_EMPTY_ATOKEN_ADDRESS) {
      if (params.functionName === 'symbol') return '';
      if (params.functionName === 'name') return 'Neverland Interest Bearing ';
      throw new Error('empty aToken metadata');
    }
    if (params.address === VIEM_NAME_ONLY_ADDRESS) {
      if (params.functionName === 'name') return 'NameOnly';
      throw new Error('name only');
    }
    if (params.functionName === 'balanceOf') {
      const owner = typeof params.args?.[0] === 'string' ? params.args[0].toLowerCase() : 'unknown';
      const key = `${params.address.toLowerCase()}:${owner}`;
      if (lpBalanceOverrides.has(key)) {
        return lpBalanceOverrides.get(key);
      }
    }
    if (params.functionName === 'tokenOfOwnerByIndex') {
      const owner = typeof params.args?.[0] === 'string' ? params.args[0].toLowerCase() : 'unknown';
      const index = params.args?.[1];
      const key = `${params.address.toLowerCase()}:${owner}`;
      if (lpTokenOverrides.has(key)) {
        const tokens = lpTokenOverrides.get(key);
        if (!tokens) {
          return null;
        }
        const idx = typeof index === 'bigint' ? Number(index) : 0;
        if (idx >= tokens.length) {
          throw new Error('token index out of range');
        }
        return tokens[idx];
      }
    }
    if (params.functionName === 'positions') {
      if (lpPositionOverride !== undefined) {
        return lpPositionOverride ?? 0n;
      }
    }
    if (params.functionName === 'slot0') {
      return [0n, 0, 0, 0, 0, 0, true];
    }
    if (params.functionName === 'fee') {
      return 3000;
    }

    switch (params.functionName) {
      case 'balanceOf':
        return 123n;
      case 'getActivePartnerships':
        if (params.address === NFT_PARTNERSHIP_REGISTRY_ADDRESS) {
          if (activePartnershipsOverride !== undefined) {
            return activePartnershipsOverride;
          }
          return [
            VIEM_PARTIAL_ADDRESS,
            VIEM_SECOND_NFT_ADDRESS,
            VIEM_ZERO_BALANCE_ADDRESS,
            VIEM_NO_NFT_ADDRESS,
            VIEM_ERROR_ADDRESS,
          ];
        }
        return [VIEM_PARTIAL_ADDRESS];
      case 'balanceOfNFT':
        return 456n;
      case 'decimals':
        return 6;
      case 'symbol':
        return 'TST';
      case 'name':
        return 'Test Token';
      default:
        return 0n;
    }
  };
}
