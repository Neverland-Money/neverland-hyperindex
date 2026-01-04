import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

// Disable bootstrap in tests
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import { POOL_CONFIGURATOR_ID, POOL_ID, ZERO_ADDRESS } from '../helpers/constants';
import {
  VIEM_ATOKEN_ADDRESS,
  VIEM_EMPTY_ATOKEN_ADDRESS,
  VIEM_ERROR_ADDRESS,
  VIEM_NAME_ONLY_ADDRESS,
  VIEM_PARTIAL_ADDRESS,
  installViemMock,
} from './viem-mock';

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';
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

function loadTestHelpers() {
  const cwd = process.cwd();
  const distTestRoot = path.join(cwd, 'dist-test');
  const generatedLink = path.join(distTestRoot, 'generated');

  const generatedIndex = path.join(generatedLink, 'index.js');
  if (!fs.existsSync(generatedIndex)) {
    if (fs.existsSync(generatedLink)) {
      fs.rmSync(generatedLink, { recursive: true, force: true });
    }
    fs.symlinkSync(path.join(cwd, 'generated'), generatedLink, 'dir');
  }

  const handlerModules = [
    'tokenization',
    'leaderboard',
    'leaderboardKeeper',
    'dustlock',
    'pool',
    'nft',
    'config',
    'rewards',
  ];
  for (const handler of handlerModules) {
    require(path.join(distTestRoot, 'src', 'handlers', `${handler}.js`));
  }

  return require(path.join(cwd, 'generated', 'src', 'TestHelpers.res.js'));
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
    newAddress: ADDRESSES.pool,
    ...eventData(5, 1040, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.PoolAddressesProvider.PoolUpdated.processEvent({
    event: poolUpdate,
    mockDb,
  });

  const configuratorUpdate =
    TestHelpers.PoolAddressesProvider.PoolConfiguratorUpdated.createMockEvent({
      oldAddress: ZERO_ADDRESS,
      newAddress: ADDRESSES.configurator,
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
  assert.equal(pool?.poolConfigurator, ADDRESSES.configurator);
  assert.equal(pool?.proxyPriceProvider, ADDRESSES.priceOracle);
  assert.equal(pool?.poolDataProviderImpl, ADDRESSES.dataProvider);
  assert.ok(mockDb.entities.PriceOracle.get(ADDRESSES.provider));
  assert.equal(
    mockDb.entities.ContractToPoolMapping.get(ADDRESSES.configurator)?.pool_id,
    ADDRESSES.provider
  );
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
  assert.equal(reserve?.symbol, 'ABC');
  assert.equal(reserve?.name, 'ABC');
  assert.equal(reserve?.decimals, 6);
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
  assert.equal(reserve?.symbol, 'PART');
  assert.equal(reserve?.name, 'PART');

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
  assert.equal(reserve?.symbol, 'NameOnly');
  assert.equal(reserve?.name, 'NameOnly');
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
