// Viem has its own types - this module declaration is only for fallback
declare module 'viem' {
  export type Address = string;

  export type Chain = {
    id: number;
    name: string;
    nativeCurrency: { name: string; symbol: string; decimals: number };
    rpcUrls: { default: { http: string[] } };
    blockExplorers?: { default: { name: string; url: string } };
  };

  export type Transport = { readonly kind?: unknown };

  export type ReadContractParams = {
    address: Address;
    abi: readonly unknown[];
    functionName: string;
    args?: readonly unknown[];
    blockNumber?: bigint;
  };

  export type PublicClient = {
    readContract: (params: ReadContractParams) => Promise<unknown>;
  };

  export const createPublicClient: (config: { chain: Chain; transport: Transport }) => PublicClient;
  export const http: (url?: string) => Transport;
  export const defineChain: (chain: Chain) => Chain;
}
