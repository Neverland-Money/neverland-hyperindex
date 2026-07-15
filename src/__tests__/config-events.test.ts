import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';

// Disable bootstrap in tests
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import {
  AUSD_ADDRESS,
  POOL_CONFIGURATOR_ID,
  POOL_ID,
  PT_AUSD_8OCT2026_ADDRESS,
  ZERO_ADDRESS,
} from '../helpers/constants';
import {
  VIEM_ATOKEN_ADDRESS,
  VIEM_EMPTY_ATOKEN_ADDRESS,
  VIEM_ERROR_ADDRESS,
  VIEM_NAME_ONLY_ADDRESS,
  VIEM_PARTIAL_ADDRESS,
  installViemMock,
} from './viem-mock';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
installViemMock();

const ADDRESSES = {
  registry: '0x0000000000000000000000000000000000009001',
  provider: '0x0000000000000000000000000000000000009002',
  pool: '0x0000000000000000000000000000000000009003',
  configurator: '0x0000000000000000000000000000000000009004',
  priceOracle: '0x0000000000000000000000000000000000009005',
  dataProvider: '0x0000000000000000000000000000000000009006',
  poolImpl: '0x0000000000000000000000000000000000009007',
  configImpl: '0x0000000000000000000000000000000000009008',
  asset: '0x0000000000000000000000000000000000009009',
  assetTwo: VIEM_PARTIAL_ADDRESS,
  assetThree: VIEM_NAME_ONLY_ADDRESS,
  aToken: VIEM_ATOKEN_ADDRESS,
  aTokenTwo: '0x0000000000000000000000000000000000009010',
  vToken: '0x0000000000000000000000000000000000009011',
  sToken: '0x0000000000000000000000000000000000009012',
  interestStrategy: '0x0000000000000000000000000000000000009013',
  eModeOracle: '0x0000000000000000000000000000000000009014',
  poolAdmin: '0x0000000000000000000000000000000000009015',
  emergencyAdmin: '0x0000000000000000000000000000000000009016',
  newStrategy: '0x0000000000000000000000000000000000009017',
  vaultMissing: '0x0000000000000000000000000000000000009018',
};

const PENDLE_AUSD_PROVIDER = '0xb80397a931fcfda3ac999a3a5639c328dc72a58f';
const CANONICAL_PROVIDER = '0x49d75170f55c964dfdd6726c74fdedee75553a0f';
const PROVIDER_REGISTRY = '0xd0ccde10cacd12f1c839db6400b82a82ab90fa9b';

function extractYamlContractBlock(configText: string, contractName: string): string {
  const lines = configText.split('\n');
  const start = lines.findIndex(line =>
    new RegExp(`^\\s*- name:\\s*${contractName}\\s*$`).test(line)
  );
  assert.notEqual(start, -1, `${contractName} is missing from config.yaml`);

  const blockLines = [lines[start]];
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i];
    if (/^\s{6}- name:\s+/.test(line)) break;
    blockLines.push(line);
  }

  return blockLines.join('\n');
}

function readYamlAddressList(block: string): string[] {
  const lines = block.split('\n');
  const addressKeyIndex = lines.findIndex(line => /^\s*address:\s*$/.test(line));
  assert.notEqual(addressKeyIndex, -1, 'PoolAddressesProvider address list is missing');

  const addresses: string[] = [];
  for (const line of lines.slice(addressKeyIndex + 1)) {
    if (/^\s{8}[a-zA-Z_][\w-]*:/.test(line)) break;
    const match = line.match(/^\s*-\s*(0x[0-9a-fA-F]{40})\b/);
    if (match) addresses.push(match[1].toLowerCase());
  }

  return addresses;
}

function loadTestHelpers() {
  return TestHelpers;
}

function createEventDataFactory() {
  let counter = 1;
  return (blockNumber: number, timestamp: number, srcAddress: string) => {
    const txHash = `0x${counter.toString(16).padStart(64, '0')}`;
    const mockEventData = {
      block: { number: blockNumber, timestamp },
      logIndex: counter,
      srcAddress,
      transaction: { hash: txHash },
    };
    counter += 1;
    return { mockEventData };
  };
}

test('addresses provider registry updates pools and contracts', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const register =
    TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.createMockEvent({
      addressesProvider: ADDRESSES.provider,
      id: 1n,
      ...eventData(1, 1000, ADDRESSES.registry),
    });
  mockDb = await TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.processEvent(
    {
      event: register,
      mockDb,
    }
  );

  const unregister =
    TestHelpers.PoolAddressesProviderRegistry.AddressesProviderUnregistered.createMockEvent({
      addressesProvider: ADDRESSES.provider,
      ...eventData(2, 1010, ADDRESSES.registry),
    });
  mockDb =
    await TestHelpers.PoolAddressesProviderRegistry.AddressesProviderUnregistered.processEvent({
      event: unregister,
      mockDb,
    });

  const proxyPool = TestHelpers.PoolAddressesProvider.ProxyCreated.createMockEvent({
    id: POOL_ID,
    proxyAddress: ADDRESSES.pool,
    implementationAddress: ADDRESSES.poolImpl,
    ...eventData(3, 1020, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.ProxyCreated.processEvent({
    event: proxyPool,
    mockDb,
  });

  const proxyConfigurator = TestHelpers.PoolAddressesProvider.ProxyCreated.createMockEvent({
    id: POOL_CONFIGURATOR_ID,
    proxyAddress: ADDRESSES.configurator,
    implementationAddress: ADDRESSES.configImpl,
    ...eventData(4, 1030, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.ProxyCreated.processEvent({
    event: proxyConfigurator,
    mockDb,
  });

  const poolUpdate = TestHelpers.PoolAddressesProvider.PoolUpdated.createMockEvent({
    oldAddress: ZERO_ADDRESS,
    newAddress: ADDRESSES.poolImpl,
    ...eventData(5, 1040, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolUpdated.processEvent({
    event: poolUpdate,
    mockDb,
  });

  const configuratorUpdate =
    TestHelpers.PoolAddressesProvider.PoolConfiguratorUpdated.createMockEvent({
      oldAddress: ZERO_ADDRESS,
      newAddress: ADDRESSES.configImpl,
      ...eventData(6, 1050, ADDRESSES.provider),
    });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolConfiguratorUpdated.processEvent({
    event: configuratorUpdate,
    mockDb,
  });

  const priceOracleUpdate = TestHelpers.PoolAddressesProvider.PriceOracleUpdated.createMockEvent({
    oldAddress: ZERO_ADDRESS,
    newAddress: ADDRESSES.priceOracle,
    ...eventData(7, 1060, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.PriceOracleUpdated.processEvent({
    event: priceOracleUpdate,
    mockDb,
  });

  const dataProviderUpdate =
    TestHelpers.PoolAddressesProvider.PoolDataProviderUpdated.createMockEvent({
      oldAddress: ZERO_ADDRESS,
      newAddress: ADDRESSES.dataProvider,
      ...eventData(8, 1070, ADDRESSES.provider),
    });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolDataProviderUpdated.processEvent({
    event: dataProviderUpdate,
    mockDb,
  });

  const pool = mockDb.entities.Pool.get(ADDRESSES.provider);
  assert.equal(pool?.active, false);
  assert.equal(pool?.pool, ADDRESSES.pool);
  assert.equal(pool?.poolImpl, ADDRESSES.poolImpl);
  assert.equal(pool?.poolConfigurator, ADDRESSES.configurator);
  assert.equal(pool?.poolConfiguratorImpl, ADDRESSES.configImpl);
  assert.equal(pool?.proxyPriceProvider, ADDRESSES.priceOracle);
  assert.equal(pool?.poolDataProviderImpl, ADDRESSES.dataProvider);
  assert.ok(mockDb.entities.PriceOracle.get(ADDRESSES.provider));
  assert.equal(
    mockDb.entities.ContractToPoolMapping.get(ADDRESSES.configurator)?.pool_id,
    ADDRESSES.provider
  );
});

// Additive multi-pool invariant: the isolated neverland-pendle-ausd pool is a
// second Aave market that must be tracked exactly like canonical. AUSD is listed
// in BOTH pools with DIFFERENT aTokens, so the `${asset}-${poolId}` reserve key
// and per-aToken SubToken rows must keep the two AUSD reserves fully separate
// (no overwrite / no double-write of canonical data). PT-AUSD must resolve its
// known metadata. Leaderboard aggregation stays pool-agnostic by construction.
test('two pools list the same AUSD asset without colliding; PT-AUSD resolves known metadata', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const poolIdA = '0x0000000000000000000000000000000000009020'; // canonical market
  const configuratorA = '0x0000000000000000000000000000000000009021';
  const aTokenAusdA = '0x0000000000000000000000000000000000009022';
  const vTokenAusdA = '0x0000000000000000000000000000000000009023';
  const poolIdB = '0x0000000000000000000000000000000000009024'; // isolated pendle market
  const configuratorB = '0x0000000000000000000000000000000000009025';
  const aTokenAusdB = '0x0000000000000000000000000000000000009026';
  const vTokenAusdB = '0x0000000000000000000000000000000000009027';
  const aTokenPt = '0x0000000000000000000000000000000000009028';
  const vTokenPt = '0x0000000000000000000000000000000000009029';

  const poolRow = (id: string, providerId: bigint, configurator: string) => ({
    id,
    addressProviderId: providerId,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: configurator,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: 1000,
  });

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set(poolRow(poolIdA, 1n, configuratorA));
  mockDb = mockDb.entities.Pool.set(poolRow(poolIdB, 2n, configuratorB));
  mockDb = mockDb.entities.ContractToPoolMapping.set({ id: configuratorA, pool_id: poolIdA });
  mockDb = mockDb.entities.ContractToPoolMapping.set({ id: configuratorB, pool_id: poolIdB });

  const initReserve = async (
    configurator: string,
    asset: string,
    aToken: string,
    vToken: string,
    block: number
  ) => {
    const init = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
      asset,
      aToken,
      stableDebtToken: ZERO_ADDRESS,
      variableDebtToken: vToken,
      interestRateStrategyAddress: ADDRESSES.interestStrategy,
      ...eventData(block, block * 10, configurator),
    });
    mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
      event: init,
      mockDb,
    });
  };

  // AUSD listed in BOTH markets (distinct aTokens), then PT-AUSD in the isolated one.
  await initReserve(configuratorA, AUSD_ADDRESS, aTokenAusdA, vTokenAusdA, 10);
  await initReserve(configuratorB, AUSD_ADDRESS, aTokenAusdB, vTokenAusdB, 11);
  await initReserve(configuratorB, PT_AUSD_8OCT2026_ADDRESS, aTokenPt, vTokenPt, 12);

  // The two AUSD reserves are distinct rows keyed by `${asset}-${pool}` and do not collide.
  const reserveAusdA = mockDb.entities.Reserve.get(`${AUSD_ADDRESS}-${poolIdA}`);
  const reserveAusdB = mockDb.entities.Reserve.get(`${AUSD_ADDRESS}-${poolIdB}`);
  assert.ok(reserveAusdA, 'canonical AUSD reserve exists');
  assert.ok(reserveAusdB, 'isolated AUSD reserve exists');
  assert.notEqual(reserveAusdA?.id, reserveAusdB?.id);
  assert.equal(reserveAusdA?.aToken_id, aTokenAusdA);
  assert.equal(reserveAusdB?.aToken_id, aTokenAusdB);
  assert.equal(reserveAusdA?.symbol, 'AUSD');
  assert.equal(reserveAusdB?.symbol, 'AUSD');

  // Per-pool SubToken rows route each aToken to its own market.
  assert.equal(mockDb.entities.SubToken.get(aTokenAusdA)?.pool_id, poolIdA);
  assert.equal(mockDb.entities.SubToken.get(aTokenAusdB)?.pool_id, poolIdB);

  // PT-AUSD resolves its hardcoded known metadata (6 decimals — not the 18-decimal fallback).
  const reservePt = mockDb.entities.Reserve.get(`${PT_AUSD_8OCT2026_ADDRESS}-${poolIdB}`);
  assert.equal(reservePt?.symbol, 'PT-AUSD-8OCT2026');
  assert.equal(reservePt?.name, 'PT AUSD 8OCT2026');
  assert.equal(reservePt?.decimals, 6);
});

test('Pendle isolated provider static bootstrap is a single explicit address slot', () => {
  const configText = readFileSync('config.yaml', 'utf8');
  const providerBlock = extractYamlContractBlock(configText, 'PoolAddressesProvider');
  const addresses = readYamlAddressList(providerBlock);

  assert.equal(
    new Set(addresses).size,
    addresses.length,
    'PoolAddressesProvider bootstrap addresses must not contain duplicates'
  );
  assert.equal(
    addresses.length,
    1,
    'neverland-pendle-ausd must bootstrap exactly one isolated PoolAddressesProvider address'
  );
  assert.notEqual(
    addresses[0],
    CANONICAL_PROVIDER,
    'canonical PoolAddressesProvider must remain registry-discovered, not statically bootstrapped'
  );
  assert.notEqual(
    addresses[0],
    PROVIDER_REGISTRY,
    'provider registry address is not an isolated PoolAddressesProvider'
  );
  assert.notEqual(
    addresses[0],
    ZERO_ADDRESS,
    'isolated PoolAddressesProvider bootstrap address must not be zero'
  );
  assert.equal(
    addresses[0],
    PENDLE_AUSD_PROVIDER,
    'bootstrap address must be the deployed PoolAddressesProvider-neverland-pendle-ausd'
  );
});

test('static provider bootstrap preserves isolated pool events before registry registration', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const proxyPool = TestHelpers.PoolAddressesProvider.ProxyCreated.createMockEvent({
    id: POOL_ID,
    proxyAddress: ADDRESSES.pool,
    implementationAddress: ADDRESSES.poolImpl,
    ...eventData(19, 1190, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.ProxyCreated.processEvent({
    event: proxyPool,
    mockDb,
  });

  const poolUpdate = TestHelpers.PoolAddressesProvider.PoolUpdated.createMockEvent({
    oldAddress: ZERO_ADDRESS,
    newAddress: ADDRESSES.poolImpl,
    ...eventData(20, 1200, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolUpdated.processEvent({
    event: poolUpdate,
    mockDb,
  });

  const proxyConfigurator = TestHelpers.PoolAddressesProvider.ProxyCreated.createMockEvent({
    id: POOL_CONFIGURATOR_ID,
    proxyAddress: ADDRESSES.configurator,
    implementationAddress: ADDRESSES.configImpl,
    ...eventData(21, 1210, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.ProxyCreated.processEvent({
    event: proxyConfigurator,
    mockDb,
  });

  const configuratorUpdate =
    TestHelpers.PoolAddressesProvider.PoolConfiguratorUpdated.createMockEvent({
      oldAddress: ZERO_ADDRESS,
      newAddress: ADDRESSES.configurator,
      ...eventData(22, 1220, ADDRESSES.provider),
    });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolConfiguratorUpdated.processEvent({
    event: configuratorUpdate,
    mockDb,
  });

  const priceOracleUpdate = TestHelpers.PoolAddressesProvider.PriceOracleUpdated.createMockEvent({
    oldAddress: ZERO_ADDRESS,
    newAddress: ADDRESSES.priceOracle,
    ...eventData(23, 1230, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.PriceOracleUpdated.processEvent({
    event: priceOracleUpdate,
    mockDb,
  });

  const dataProviderUpdate =
    TestHelpers.PoolAddressesProvider.PoolDataProviderUpdated.createMockEvent({
      oldAddress: ZERO_ADDRESS,
      newAddress: ADDRESSES.dataProvider,
      ...eventData(24, 1240, ADDRESSES.provider),
    });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolDataProviderUpdated.processEvent({
    event: dataProviderUpdate,
    mockDb,
  });

  let pool = mockDb.entities.Pool.get(ADDRESSES.provider);
  assert.equal(pool?.addressProviderId, 0n);
  assert.equal(pool?.pool, ADDRESSES.pool);
  assert.equal(pool?.poolImpl, ADDRESSES.poolImpl);
  assert.equal(pool?.poolConfigurator, ADDRESSES.configurator);
  assert.equal(pool?.proxyPriceProvider, ADDRESSES.priceOracle);
  assert.equal(pool?.poolDataProviderImpl, ADDRESSES.dataProvider);
  assert.equal(
    mockDb.entities.ContractToPoolMapping.get(ADDRESSES.configurator)?.pool_id,
    ADDRESSES.provider
  );

  const register =
    TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.createMockEvent({
      addressesProvider: ADDRESSES.provider,
      id: 2n,
      ...eventData(25, 1250, ADDRESSES.registry),
    });
  mockDb = await TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.processEvent(
    {
      event: register,
      mockDb,
    }
  );

  pool = mockDb.entities.Pool.get(ADDRESSES.provider);
  assert.equal(pool?.addressProviderId, 2n);
  assert.equal(pool?.pool, ADDRESSES.pool);
  assert.equal(pool?.poolConfigurator, ADDRESSES.configurator);
  assert.equal(pool?.proxyPriceProvider, ADDRESSES.priceOracle);
  assert.equal(pool?.poolDataProviderImpl, ADDRESSES.dataProvider);

  const init = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: AUSD_ADDRESS,
    aToken: ADDRESSES.aToken,
    stableDebtToken: ZERO_ADDRESS,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(26, 1260, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: init,
    mockDb,
  });

  const reserve = mockDb.entities.Reserve.get(`${AUSD_ADDRESS}-${ADDRESSES.provider}`);
  assert.equal(reserve?.pool_id, ADDRESSES.provider);
  assert.equal(reserve?.symbol, 'AUSD');
});

test('pool configurator events update reserves and configuration history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 1n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: ADDRESSES.configurator,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: 1000,
  });
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.configurator,
    pool_id: ADDRESSES.pool,
  });

  const init = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    aToken: ADDRESSES.aToken,
    stableDebtToken: ADDRESSES.sToken,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(10, 1100, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: init,
    mockDb,
  });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'ERC20');
  assert.equal(reserve?.name, 'Token ERC20');
  assert.equal(reserve?.decimals, 18);
  assert.ok(mockDb.entities.SubToken.get(ADDRESSES.sToken));

  const initUnderlying = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: ADDRESSES.assetTwo,
    aToken: ADDRESSES.aTokenTwo,
    stableDebtToken: ZERO_ADDRESS,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(11, 1110, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initUnderlying,
    mockDb,
  });

  const initNameOnly = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: ADDRESSES.assetThree,
    aToken: ADDRESSES.aTokenTwo,
    stableDebtToken: ZERO_ADDRESS,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(12, 1120, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initNameOnly,
    mockDb,
  });

  const borrowing = TestHelpers.PoolConfigurator.ReserveBorrowing.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    enabled: true,
    ...eventData(13, 1130, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveBorrowing.processEvent({
    event: borrowing,
    mockDb,
  });

  const collateral = TestHelpers.PoolConfigurator.CollateralConfigurationChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    ltv: 5000n,
    liquidationThreshold: 6000n,
    liquidationBonus: 10500n,
    ...eventData(14, 1140, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.CollateralConfigurationChanged.processEvent({
    event: collateral,
    mockDb,
  });

  const stableBorrowing = TestHelpers.PoolConfigurator.ReserveStableRateBorrowing.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    enabled: true,
    ...eventData(15, 1150, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveStableRateBorrowing.processEvent({
    event: stableBorrowing,
    mockDb,
  });

  const active = TestHelpers.PoolConfigurator.ReserveActive.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    active: false,
    ...eventData(16, 1160, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveActive.processEvent({
    event: active,
    mockDb,
  });

  const frozen = TestHelpers.PoolConfigurator.ReserveFrozen.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    frozen: true,
    ...eventData(17, 1170, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveFrozen.processEvent({
    event: frozen,
    mockDb,
  });

  const paused = TestHelpers.PoolConfigurator.ReservePaused.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    paused: true,
    ...eventData(18, 1180, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReservePaused.processEvent({
    event: paused,
    mockDb,
  });

  const dropped = TestHelpers.PoolConfigurator.ReserveDropped.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    ...eventData(19, 1190, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropped,
    mockDb,
  });

  const reserveFactor = TestHelpers.PoolConfigurator.ReserveFactorChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newReserveFactor: 100n,
    ...eventData(20, 1200, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveFactorChanged.processEvent({
    event: reserveFactor,
    mockDb,
  });

  const supplyCap = TestHelpers.PoolConfigurator.SupplyCapChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newSupplyCap: 500n,
    ...eventData(21, 1210, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.SupplyCapChanged.processEvent({
    event: supplyCap,
    mockDb,
  });

  const borrowCap = TestHelpers.PoolConfigurator.BorrowCapChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newBorrowCap: 250n,
    ...eventData(22, 1220, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.BorrowCapChanged.processEvent({
    event: borrowCap,
    mockDb,
  });

  const eModeChanged = TestHelpers.PoolConfigurator.EModeAssetCategoryChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newCategoryId: 1n,
    ...eventData(23, 1230, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.EModeAssetCategoryChanged.processEvent({
    event: eModeChanged,
    mockDb,
  });

  const eModeAdded = TestHelpers.PoolConfigurator.EModeCategoryAdded.createMockEvent({
    categoryId: 1n,
    ltv: 7000n,
    liquidationThreshold: 7500n,
    liquidationBonus: 10300n,
    oracle: ADDRESSES.eModeOracle,
    label: 'emode',
    ...eventData(24, 1240, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.EModeCategoryAdded.processEvent({
    event: eModeAdded,
    mockDb,
  });

  const aTokenUpgrade = TestHelpers.PoolConfigurator.ATokenUpgraded.createMockEvent({
    proxy: ADDRESSES.aToken,
    implementation: ADDRESSES.poolImpl,
    ...eventData(25, 1250, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ATokenUpgraded.processEvent({
    event: aTokenUpgrade,
    mockDb,
  });

  const stableUpgrade = TestHelpers.PoolConfigurator.StableDebtTokenUpgraded.createMockEvent({
    proxy: ADDRESSES.sToken,
    implementation: ADDRESSES.poolImpl,
    ...eventData(26, 1260, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.StableDebtTokenUpgraded.processEvent({
    event: stableUpgrade,
    mockDb,
  });

  const variableUpgrade = TestHelpers.PoolConfigurator.VariableDebtTokenUpgraded.createMockEvent({
    proxy: ADDRESSES.vToken,
    implementation: ADDRESSES.poolImpl,
    ...eventData(27, 1270, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.VariableDebtTokenUpgraded.processEvent({
    event: variableUpgrade,
    mockDb,
  });

  const borrowable = TestHelpers.PoolConfigurator.BorrowableInIsolationChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    borrowable: true,
    ...eventData(28, 1280, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.BorrowableInIsolationChanged.processEvent({
    event: borrowable,
    mockDb,
  });

  const siloed = TestHelpers.PoolConfigurator.SiloedBorrowingChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newState: true,
    ...eventData(29, 1290, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.SiloedBorrowingChanged.processEvent({
    event: siloed,
    mockDb,
  });

  const debtCeiling = TestHelpers.PoolConfigurator.DebtCeilingChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newDebtCeiling: 100n,
    ...eventData(30, 1300, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.DebtCeilingChanged.processEvent({
    event: debtCeiling,
    mockDb,
  });

  const unbacked = TestHelpers.PoolConfigurator.UnbackedMintCapChanged.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    newUnbackedMintCap: 50n,
    ...eventData(31, 1310, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.UnbackedMintCapChanged.processEvent({
    event: unbacked,
    mockDb,
  });

  const liquidationFee = TestHelpers.PoolConfigurator.LiquidationProtocolFeeChanged.createMockEvent(
    {
      asset: VIEM_ERROR_ADDRESS,
      newFee: 250n,
      ...eventData(32, 1320, ADDRESSES.configurator),
    }
  );
  mockDb = await TestHelpers.PoolConfigurator.LiquidationProtocolFeeChanged.processEvent({
    event: liquidationFee,
    mockDb,
  });

  const strategyChange =
    TestHelpers.PoolConfigurator.ReserveInterestRateStrategyChanged.createMockEvent({
      asset: VIEM_ERROR_ADDRESS,
      newStrategy: ADDRESSES.newStrategy,
      ...eventData(33, 1330, ADDRESSES.configurator),
    });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInterestRateStrategyChanged.processEvent({
    event: strategyChange,
    mockDb,
  });

  const premiumTotal = TestHelpers.PoolConfigurator.FlashloanPremiumTotalUpdated.createMockEvent({
    newFlashloanPremiumTotal: 5n,
    ...eventData(34, 1340, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.FlashloanPremiumTotalUpdated.processEvent({
    event: premiumTotal,
    mockDb,
  });

  const premiumProtocol =
    TestHelpers.PoolConfigurator.FlashloanPremiumToProtocolUpdated.createMockEvent({
      newFlashloanPremiumToProtocol: 3n,
      ...eventData(35, 1350, ADDRESSES.configurator),
    });
  mockDb = await TestHelpers.PoolConfigurator.FlashloanPremiumToProtocolUpdated.processEvent({
    event: premiumProtocol,
    mockDb,
  });

  const collateralInEmode =
    TestHelpers.PoolConfigurator.AssetCollateralInEModeChanged.createMockEvent({
      asset: VIEM_ERROR_ADDRESS,
      categoryId: 1n,
      collateral: true,
      ...eventData(36, 1360, ADDRESSES.configurator),
    });
  mockDb = await TestHelpers.PoolConfigurator.AssetCollateralInEModeChanged.processEvent({
    event: collateralInEmode,
    mockDb,
  });

  const borrowableInEmode =
    TestHelpers.PoolConfigurator.AssetBorrowableInEModeChanged.createMockEvent({
      asset: VIEM_ERROR_ADDRESS,
      categoryId: 1n,
      borrowable: true,
      ...eventData(37, 1370, ADDRESSES.configurator),
    });
  mockDb = await TestHelpers.PoolConfigurator.AssetBorrowableInEModeChanged.processEvent({
    event: borrowableInEmode,
    mockDb,
  });

  const borrowableInEmodeNew =
    TestHelpers.PoolConfigurator.AssetBorrowableInEModeChanged.createMockEvent({
      asset: ADDRESSES.assetTwo,
      categoryId: 2n,
      borrowable: true,
      ...eventData(38, 1375, ADDRESSES.configurator),
    });
  mockDb = await TestHelpers.PoolConfigurator.AssetBorrowableInEModeChanged.processEvent({
    event: borrowableInEmodeNew,
    mockDb,
  });

  const bridgeFee = TestHelpers.PoolConfigurator.BridgeProtocolFeeUpdated.createMockEvent({
    newBridgeProtocolFee: 7n,
    ...eventData(39, 1380, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.BridgeProtocolFeeUpdated.processEvent({
    event: bridgeFee,
    mockDb,
  });

  const reserveUpdated = mockDb.entities.Reserve.get(reserveId);
  assert.ok(reserveUpdated?.borrowableInIsolation);
  assert.equal(mockDb.entities.Pool.get(ADDRESSES.pool)?.flashloanPremiumTotal, 5n);
  assert.equal(mockDb.entities.Pool.get(ADDRESSES.pool)?.flashloanPremiumToProtocol, 3n);
  assert.ok(mockDb.entities.EModeCategory.get('1'));
  assert.ok(mockDb.entities.EModeCategoryConfig.get(`${VIEM_ERROR_ADDRESS}-1`));
  assert.ok(mockDb.entities.EModeCategoryConfig.get(`${ADDRESSES.assetTwo}-2`));
});

test('reserve initialized falls back when aToken metadata collapses to empty', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 1n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: ADDRESSES.configurator,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: 1200,
  });
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.configurator,
    pool_id: ADDRESSES.pool,
  });

  const init = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    aToken: VIEM_EMPTY_ATOKEN_ADDRESS,
    stableDebtToken: ADDRESSES.sToken,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(40, 1200, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: init,
    mockDb,
  });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'ERC20');
  assert.equal(reserve?.name, 'Token ERC20');
});

test('reserve initialized handles null and partial aToken metadata', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 1n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: ADDRESSES.configurator,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: 1300,
  });
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.configurator,
    pool_id: ADDRESSES.pool,
  });

  const initNull = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    aToken: VIEM_ERROR_ADDRESS,
    stableDebtToken: ADDRESSES.sToken,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(41, 1300, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initNull,
    mockDb,
  });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  let reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'ERC20');
  assert.equal(reserve?.name, 'Token ERC20');

  const initSymbolOnly = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    aToken: VIEM_PARTIAL_ADDRESS,
    stableDebtToken: ADDRESSES.sToken,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(42, 1310, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initSymbolOnly,
    mockDb,
  });

  reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'ERC20');
  assert.equal(reserve?.name, 'Token ERC20');

  const initNameOnly = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_ERROR_ADDRESS,
    aToken: VIEM_NAME_ONLY_ADDRESS,
    stableDebtToken: ADDRESSES.sToken,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(43, 1320, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initNameOnly,
    mockDb,
  });

  reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'ERC20');
  assert.equal(reserve?.name, 'Token ERC20');
});

test('pool configurator uses src address when pool mapping is missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: VIEM_PARTIAL_ADDRESS,
    aToken: ADDRESSES.aToken,
    stableDebtToken: ZERO_ADDRESS,
    variableDebtToken: ADDRESSES.vToken,
    interestRateStrategyAddress: ADDRESSES.interestStrategy,
    ...eventData(30, 1300, ADDRESSES.configurator),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: init,
    mockDb,
  });

  const reserveId = `${VIEM_PARTIAL_ADDRESS}-${ADDRESSES.configurator}`;
  assert.equal(mockDb.entities.Reserve.get(reserveId)?.pool_id, ADDRESSES.configurator);
});

test('vault creation and self-repay create summaries when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const vaultCreated = TestHelpers.UserVaultFactory.UserVaultCreated.createMockEvent({
    vault: ADDRESSES.provider,
    user: ADDRESSES.poolAdmin,
    ...eventData(50, 2000, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.UserVaultFactory.UserVaultCreated.processEvent({
    event: vaultCreated,
    mockDb,
  });

  const repay = TestHelpers.UserVault.LoanSelfRepaid.createMockEvent({
    user: ADDRESSES.poolAdmin,
    debtAsset: ADDRESSES.asset,
    collateralAsset: ADDRESSES.assetThree,
    amount: 10n,
    ...eventData(51, 2010, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.UserVault.LoanSelfRepaid.processEvent({ event: repay, mockDb });

  const vault = mockDb.entities.UserVault.get(ADDRESSES.provider);
  assert.ok(vault);
  assert.equal(vault?.repayCount, 1n);
});

test('self-repay initializes missing vault summary', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const repay = TestHelpers.UserVault.LoanSelfRepaid.createMockEvent({
    user: ADDRESSES.poolAdmin,
    debtAsset: ADDRESSES.asset,
    collateralAsset: ADDRESSES.assetThree,
    amount: 22n,
    ...eventData(60, 2100, ADDRESSES.vaultMissing),
  });
  mockDb = await TestHelpers.UserVault.LoanSelfRepaid.processEvent({ event: repay, mockDb });

  const vaultSummary = mockDb.entities.UserVault.get(ADDRESSES.vaultMissing);
  assert.ok(vaultSummary);
  assert.equal(vaultSummary?.totalRepayVolume, 22n);
  assert.equal(vaultSummary?.repayCount, 1n);
});
