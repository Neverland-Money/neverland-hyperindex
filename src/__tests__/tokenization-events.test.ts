import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { KNOWN_GATEWAYS } from '../helpers/constants';
import { VIEM_ERROR_ADDRESS, installViemMock } from './viem-mock';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';
installViemMock();

const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;

const ADDRESSES = {
  stableToken: '0x0000000000000000000000000000000000004001',
  variableToken: '0x0000000000000000000000000000000000004002',
  asset: '0x0000000000000000000000000000000000004003',
  oracle: '0x0000000000000000000000000000000000004004',
  pool: '0x0000000000000000000000000000000000004005',
  fromUser: '0x0000000000000000000000000000000000004006',
  toUser: '0x0000000000000000000000000000000000004007',
  aToken: '0x0000000000000000000000000000000000004008',
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

test('borrow allowance delegated creates user reserve and delegated allowances', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.stableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: 6,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.variableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: 6,
  });

  const stableEvent = TestHelpers.StableDebtToken.BorrowAllowanceDelegated.createMockEvent({
    fromUser: ADDRESSES.fromUser,
    toUser: ADDRESSES.toUser,
    asset: ADDRESSES.asset,
    amount: 50n,
    ...eventData(1, 100, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.BorrowAllowanceDelegated.processEvent({
    event: stableEvent,
    mockDb,
  });

  const stableId = `${ADDRESSES.fromUser}-${ADDRESSES.toUser}-${ADDRESSES.asset}-stable`;
  const stableAllowance = mockDb.entities.BorrowAllowance.get(stableId);
  assert.ok(stableAllowance);
  assert.equal(stableAllowance?.amount, 50n);

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;
  assert.ok(mockDb.entities.UserReserve.get(userReserveId));

  const delegatedStableId = `stable${ADDRESSES.fromUser}${ADDRESSES.toUser}${ADDRESSES.asset}`;
  const delegatedStable = mockDb.entities.StableTokenDelegatedAllowance.get(delegatedStableId);
  assert.ok(delegatedStable);
  assert.equal(delegatedStable?.amountAllowed, 50n);

  const variableEvent = TestHelpers.VariableDebtToken.BorrowAllowanceDelegated.createMockEvent({
    fromUser: ADDRESSES.fromUser,
    toUser: ADDRESSES.toUser,
    asset: ADDRESSES.asset,
    amount: 75n,
    ...eventData(2, 110, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.BorrowAllowanceDelegated.processEvent({
    event: variableEvent,
    mockDb,
  });

  const variableId = `${ADDRESSES.fromUser}-${ADDRESSES.toUser}-${ADDRESSES.asset}-variable`;
  const variableAllowance = mockDb.entities.BorrowAllowance.get(variableId);
  assert.ok(variableAllowance);
  assert.equal(variableAllowance?.amount, 75n);

  const delegatedVariableId = `variable${ADDRESSES.fromUser}${ADDRESSES.toUser}${ADDRESSES.asset}`;
  const delegatedVariable =
    mockDb.entities.VariableTokenDelegatedAllowance.get(delegatedVariableId);
  assert.ok(delegatedVariable);
  assert.equal(delegatedVariable?.amountAllowed, 75n);

  const userList = mockDb.entities.UserReserveList.get(ADDRESSES.fromUser);
  assert.ok(userList);
  assert.equal(userList?.reserveIds.length, 1);
  assert.equal(userList?.reserveIds[0], reserveId);
});

test('price observed normalizes price and records history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const event = TestHelpers.AToken.PriceObserved.createMockEvent({
    asset: ADDRESSES.asset,
    price: 2000n,
    baseUnit: 1000n,
    oracle: ADDRESSES.oracle,
    action: 0,
    ok: false,
    user: ADDRESSES.fromUser,
    timestamp: 120,
    ...eventData(3, 120, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.AToken.PriceObserved.processEvent({ event, mockDb });

  const asset = mockDb.entities.PriceOracleAsset.get(ADDRESSES.asset);
  assert.ok(asset);
  assert.equal(asset?.priceInEth, 200000000n);
  assert.equal(asset?.lastPriceUsd, 2);
  assert.equal(asset?.isFallbackRequired, true);

  const historyId = `${ADDRESSES.asset}-3-1`;
  const history = mockDb.entities.PriceHistoryItem.get(historyId);
  assert.ok(history);
  assert.equal(history?.price, 200000000n);

  const eventTwo = TestHelpers.AToken.PriceObserved.createMockEvent({
    asset: ADDRESSES.asset,
    price: 500000000n,
    baseUnit: 100000000n,
    oracle: ADDRESSES.oracle,
    action: 0,
    ok: true,
    user: ADDRESSES.fromUser,
    timestamp: 130,
    ...eventData(4, 130, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.AToken.PriceObserved.processEvent({ event: eventTwo, mockDb });

  const updated = mockDb.entities.PriceOracleAsset.get(ADDRESSES.asset);
  assert.equal(updated?.priceInEth, 500000000n);
  assert.equal(updated?.lastPriceUsd, 5);
  assert.equal(updated?.isFallbackRequired, false);
});

test('gateway withdrawals attribute redeem to actual user', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const gateway = KNOWN_GATEWAYS[0];
  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;

  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: 100,
  });
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    lastUpdateTimestamp: 100,
    isActive: true,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.User.set({
    id: ADDRESSES.fromUser,
    totalLiquidityUSD: 0,
    totalBorrowsUSD: 0,
    totalCollateralUSD: 0,
    createdAt: 100,
    lastUpdated: 100,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 1000n * UNIT,
    currentATokenBalance: 1000n * UNIT,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 100,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const transferMeta = eventData(5, 200, ADDRESSES.aToken);
  const txHash = transferMeta.mockEventData.transaction.hash;

  const transfer = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: gateway,
    value: 100n * UNIT,
    index: RAY,
    ...transferMeta,
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transfer,
    mockDb,
  });

  const pendingId = `${txHash}:${ADDRESSES.asset}:${gateway}`;
  const pending = mockDb.entities.PendingGatewayWithdrawal.get(pendingId);
  assert.ok(pending);
  assert.equal(pending?.actualUser, ADDRESSES.fromUser);

  const burnMeta = {
    mockEventData: {
      block: transferMeta.mockEventData.block,
      logIndex: transferMeta.mockEventData.logIndex + 1,
      srcAddress: ADDRESSES.aToken,
      transaction: { hash: txHash },
    },
  };
  const burnLogIndex = burnMeta.mockEventData.logIndex;
  const burn = TestHelpers.AToken.Burn.createMockEvent({
    from: gateway,
    target: gateway,
    value: 100n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...burnMeta,
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burn, mockDb });

  const redeemId = `${txHash}:${burnLogIndex}:${userReserveId}`;
  const redeem = mockDb.entities.RedeemUnderlying.get(redeemId);
  assert.ok(redeem);
  assert.equal(redeem?.user_id, ADDRESSES.fromUser);
  assert.equal(redeem?.userReserve_id, userReserveId);

  const pendingAfter = mockDb.entities.PendingGatewayWithdrawal.get(pendingId);
  assert.equal(pendingAfter, undefined);
});

test('aToken burn updates balances and reserve totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;

  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
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
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalATokenSupply: 1000n,
    availableLiquidity: 1000n,
    totalLiquidity: 1000n,
    totalLiquidityAsCollateral: 200n,
    totalSupplies: 1000n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 1000,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 1000n,
    currentATokenBalance: 1000n,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const burn = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(30, 2000, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burn, mockDb });

  const updatedUserReserve = mockDb.entities.UserReserve.get(userReserveId);
  assert.equal(updatedUserReserve?.scaledATokenBalance, 900n);
  assert.equal(updatedUserReserve?.currentATokenBalance, 900n);

  const updatedReserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(updatedReserve?.totalATokenSupply, 900n);
  assert.equal(updatedReserve?.availableLiquidity, 900n);
  assert.equal(updatedReserve?.totalLiquidity, 900n);
  assert.equal(updatedReserve?.totalLiquidityAsCollateral, 100n);
});

test('aToken burn returns early when subtoken or pool is missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const burnNoSub = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(32, 2050, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burnNoSub, mockDb });

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  const burnNoPool = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(33, 2060, ADDRESSES.aToken),
  });
  await TestHelpers.AToken.Burn.processEvent({ event: burnNoPool, mockDb });
});

test('aToken burn skips user reserve updates when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
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
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalATokenSupply: 100n,
    availableLiquidity: 100n,
    totalLiquidity: 100n,
    totalLiquidityAsCollateral: 0n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const burn = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(34, 2070, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burn, mockDb });

  const historyId = `${ADDRESSES.fromUser}-${reserveId}:${burn.transaction.hash}:${burn.logIndex}`;
  assert.ok(mockDb.entities.ATokenBalanceHistoryItem.get(historyId));
});

test('aToken burn falls back to user reserve index when reserve missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
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
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 100n,
    currentATokenBalance: 100n,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 777n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 1000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const burn = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(38, 2110, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burn, mockDb });

  const updated = mockDb.entities.UserReserve.get(userReserveId);
  assert.equal(updated?.variableBorrowIndex, 777n);
});

test('aToken mint returns early when subtoken is missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const mint = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(35, 2080, ADDRESSES.aToken),
  });
  await TestHelpers.AToken.Mint.processEvent({ event: mint, mockDb });
});

test('aToken mint handles missing reserve and collateral updates', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const mintNoReserve = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    value: 10n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(36, 2090, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: mintNoReserve, mockDb });

  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalLiquidity: 100n,
    availableLiquidity: 100n,
    totalLiquidityAsCollateral: 50n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.fromUser}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const mint = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    value: 20n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(37, 2100, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: mint, mockDb });

  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.totalLiquidityAsCollateral, 70n);
});

test('balance transfer skips when subtoken is missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const transfer = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: ADDRESSES.toUser,
    value: 10n,
    index: RAY,
    ...eventData(31, 2100, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transfer,
    mockDb,
  });

  assert.equal(mockDb.entities.UserReserve.get(`${ADDRESSES.fromUser}-unknown`), undefined);
});

test('balance transfers create user reserves and adjust collateral totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalLiquidity: 1000n,
    availableLiquidity: 1000n,
    totalLiquidityAsCollateral: 500n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 1000,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const transferCreate = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: ADDRESSES.toUser,
    value: 100n,
    index: RAY,
    ...eventData(20, 2000, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transferCreate,
    mockDb,
  });

  const fromId = `${ADDRESSES.fromUser}-${reserveId}`;
  const toId = `${ADDRESSES.toUser}-${reserveId}`;
  const fromReserve = mockDb.entities.UserReserve.get(fromId);
  const toReserve = mockDb.entities.UserReserve.get(toId);
  mockDb = mockDb.entities.UserReserve.set({
    ...fromReserve,
    usageAsCollateralEnabledOnUser: true,
  });
  mockDb = mockDb.entities.UserReserve.set({
    ...toReserve,
    usageAsCollateralEnabledOnUser: false,
  });

  const transferOut = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: ADDRESSES.toUser,
    value: 50n,
    index: RAY,
    ...eventData(21, 2010, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transferOut,
    mockDb,
  });

  let reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.totalLiquidityAsCollateral, 450n);

  const fromUpdated = mockDb.entities.UserReserve.get(fromId);
  const toUpdated = mockDb.entities.UserReserve.get(toId);
  mockDb = mockDb.entities.UserReserve.set({
    ...fromUpdated,
    usageAsCollateralEnabledOnUser: false,
  });
  mockDb = mockDb.entities.UserReserve.set({
    ...toUpdated,
    usageAsCollateralEnabledOnUser: true,
  });

  const transferIn = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: ADDRESSES.toUser,
    value: 50n,
    index: RAY,
    ...eventData(22, 2020, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transferIn,
    mockDb,
  });

  reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.totalLiquidityAsCollateral, 500n);
});

test('balance transfers clamp collateral when reserve totals are too small', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalLiquidity: 1000n,
    availableLiquidity: 1000n,
    totalLiquidityAsCollateral: 10n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 1000,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.fromUser}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 100n,
    currentATokenBalance: 100n,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
    stableBorrowLastUpdateTimestamp: 0,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.toUser}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.toUser,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 1000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const transfer = TestHelpers.AToken.BalanceTransfer.createMockEvent({
    from: ADDRESSES.fromUser,
    to: ADDRESSES.toUser,
    value: 50n,
    index: RAY,
    ...eventData(40, 2200, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.BalanceTransfer.processEvent({
    event: transfer,
    mockDb,
  });

  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.totalLiquidityAsCollateral, 0n);
});

test('aToken initialized updates reserve metadata and mapping', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.pool,
    pool_id: ADDRESSES.pool,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: VIEM_ERROR_ADDRESS,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nXYZ',
    aTokenName: 'Neverland Interest Bearing XYZ',
    ...eventData(30, 3000, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'XYZ');
  assert.equal(reserve?.name, 'XYZ');
  assert.equal(reserve?.decimals, 6);
  assert.ok(mockDb.entities.MapAssetPool.get(ADDRESSES.aToken));
});

test('variable and stable debt flows update points and reserves', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
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
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    totalLiquidity: 1000n,
    availableLiquidity: 1000n,
    lastUpdateTimestamp: 1000,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.variableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.stableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 1000,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 1,
    repayDailyBonus: 1,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });

  const priceObserved = TestHelpers.VariableDebtToken.PriceObserved.createMockEvent({
    asset: ADDRESSES.asset,
    price: 2000n,
    baseUnit: 1000n,
    oracle: ADDRESSES.oracle,
    action: 0,
    ok: true,
    user: ADDRESSES.fromUser,
    timestamp: 100,
    ...eventData(40, 100, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.PriceObserved.processEvent({
    event: priceObserved,
    mockDb,
  });

  const stablePriceObserved = TestHelpers.StableDebtToken.PriceObserved.createMockEvent({
    asset: ADDRESSES.asset,
    price: 3000n,
    baseUnit: 1000n,
    oracle: ADDRESSES.oracle,
    action: 0,
    ok: true,
    user: ADDRESSES.fromUser,
    timestamp: 110,
    ...eventData(41, 110, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.PriceObserved.processEvent({
    event: stablePriceObserved,
    mockDb,
  });

  const mint = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(42, 86400, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: mint, mockDb });

  const burn = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(43, 86410, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: burn, mockDb });

  const stableMint = TestHelpers.StableDebtToken.Mint.createMockEvent({
    user: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    amount: 50n,
    currentBalance: 0n,
    balanceIncrease: 0n,
    newRate: 2n,
    avgStableRate: 2n,
    newTotalSupply: 50n,
    ...eventData(44, 86420, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.Mint.processEvent({ event: stableMint, mockDb });

  const stableBurn = TestHelpers.StableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    amount: 50n,
    currentBalance: 50n,
    balanceIncrease: 0n,
    avgStableRate: 2n,
    newTotalSupply: 0n,
    ...eventData(45, 86430, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.Burn.processEvent({ event: stableBurn, mockDb });

  const user = mockDb.entities.User.get(ADDRESSES.fromUser);
  assert.ok(user);
  assert.equal(user?.borrowedReservesCount, 0);
});

test('debt token initialization updates subtoken mapping', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.pool,
    pool_id: ADDRESSES.pool,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.stableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.variableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const stableInit = TestHelpers.StableDebtToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    debtTokenDecimals: 6n,
    debtTokenSymbol: 'sd',
    debtTokenName: 'Stable',
    ...eventData(50, 90000, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.Initialized.processEvent({
    event: stableInit,
    mockDb,
  });

  const variableInit = TestHelpers.VariableDebtToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    debtTokenDecimals: 6n,
    debtTokenSymbol: 'vd',
    debtTokenName: 'Variable',
    ...eventData(51, 90010, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Initialized.processEvent({
    event: variableInit,
    mockDb,
  });

  assert.ok(mockDb.entities.MapAssetPool.get(ADDRESSES.stableToken));
  assert.ok(mockDb.entities.MapAssetPool.get(ADDRESSES.variableToken));
});

test('aToken initialization falls back to aToken metadata', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: VIEM_ERROR_ADDRESS,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nMOCK',
    aTokenName: '',
    ...eventData(60, 91000, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'MOCK');
  assert.equal(reserve?.name, 'MOCK');
});

test('aToken initialization copies name into symbol when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: VIEM_ERROR_ADDRESS,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: '',
    aTokenName: 'PlainToken',
    ...eventData(61, 91010, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'PlainToken');
  assert.equal(reserve?.name, 'PlainToken');
});

test('aToken initialization uses chain metadata when available', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nTST',
    aTokenName: 'Neverland Interest Bearing Test Token',
    ...eventData(64, 91040, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'TST');
  assert.equal(reserve?.name, 'Test Token');
});

test('stable debt mint initializes user reserve when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.stableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const reserve = createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset);
  mockDb = mockDb.entities.Reserve.set({
    ...reserve,
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 0,
    isActive: true,
    borrowingEnabled: true,
  });

  const stableMint = TestHelpers.StableDebtToken.Mint.createMockEvent({
    user: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    amount: 50n,
    currentBalance: 0n,
    balanceIncrease: 0n,
    newRate: 2n,
    avgStableRate: 2n,
    newTotalSupply: 50n,
    ...eventData(62, 91020, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.StableDebtToken.Mint.processEvent({ event: stableMint, mockDb });

  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;
  assert.ok(mockDb.entities.UserReserve.get(userReserveId));
  assert.ok(mockDb.entities.UserReserveList.get(ADDRESSES.fromUser));
});
