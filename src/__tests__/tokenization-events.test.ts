import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { KNOWN_GATEWAYS, WMON_ADDRESS } from '../helpers/constants';
import { VIEM_ERROR_ADDRESS, installViemMock } from './viem-mock';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
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

test('borrow allowance delegated creates user reserve and delegated allowances', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.variableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: 6,
  });

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;

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

  const variableId = `${ADDRESSES.fromUser}-${ADDRESSES.toUser}-${ADDRESSES.asset}`;
  const variableAllowance = mockDb.entities.BorrowAllowance.get(variableId);
  assert.ok(variableAllowance);
  assert.equal(variableAllowance?.amount, 75n);

  assert.ok(mockDb.entities.UserReserve.get(userReserveId));

  const delegatedVariableId = `${ADDRESSES.fromUser}${ADDRESSES.toUser}${ADDRESSES.asset}`;
  const delegatedVariable = mockDb.entities.DelegatedAllowance.get(delegatedVariableId);
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
    action: 0n,
    ok: false,
    user: ADDRESSES.fromUser,
    timestamp: 120n,
    ...eventData(3, 120, ADDRESSES.stableToken),
  });
  mockDb = await TestHelpers.AToken.PriceObserved.processEvent({ event, mockDb });

  const asset = mockDb.entities.PriceOracleAsset.get(ADDRESSES.asset);
  assert.ok(asset);
  assert.equal(asset?.priceInEth, 200000000n);
  assert.equal(asset?.lastPriceUsd, 2);
  assert.equal(asset?.isFallbackRequired, true);

  // `${asset}-${blockNumber}-${logIndex}`, with the event's block passed through unshifted by
  // v2's native mock.
  const historyId = `${ADDRESSES.asset}-3-1`;
  const history = mockDb.entities.PriceHistoryItem.get(historyId);
  assert.ok(history);
  assert.equal(history?.price, 200000000n);

  const eventTwo = TestHelpers.AToken.PriceObserved.createMockEvent({
    asset: ADDRESSES.asset,
    price: 500000000n,
    baseUnit: 100000000n,
    oracle: ADDRESSES.oracle,
    action: 0n,
    ok: true,
    user: ADDRESSES.fromUser,
    timestamp: 130n,
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
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 100,
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

  const feedRow = mockDb.entities.ReserveTx.get(redeemId);
  assert.ok(feedRow);
  assert.equal(feedRow?.kind, 'RedeemUnderlying');
  assert.equal(feedRow?.user, ADDRESSES.fromUser);
  assert.equal(feedRow?.counterparty, gateway);
  assert.equal(feedRow?.amount, 100n * UNIT);

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
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
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
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 777n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 1000,
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
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
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
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: true,
    lastUpdateTimestamp: 1000,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.toUser}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.toUser,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 1000,
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
    action: 0n,
    ok: true,
    user: ADDRESSES.fromUser,
    timestamp: 100n,
    ...eventData(40, 100, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.PriceObserved.processEvent({
    event: priceObserved,
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
    id: ADDRESSES.variableToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
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

test('aToken initialization uses event metadata without chain reads', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nTST',
    aTokenName: 'Neverland Interest Bearing TST',
    ...eventData(64, 91040, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'TST');
  // The AToken name is `${prefix} ${reserveSymbol}` and carries no long name of
  // its own, so an unknown reserve's name is its symbol. KNOWN_TOKENS is what
  // supplies a distinct long name ('Wrapped Ether' for WETH).
  assert.equal(reserve?.name, 'TST');
});

test('aToken initialization strips an isolated market prefix and keeps event decimals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  // Pendle markets deploy with SymbolPrefix 'np' and ATokenNamePrefix
  // 'Neverland Pendle'. Matching only the canonical 'n'/'Neverland Interest
  // Bearing' left these as symbol 'pSHMON', name 'Neverland Pendle SHMON'.
  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aTokenDecimals: 8n,
    aTokenSymbol: 'npSHMON',
    aTokenName: 'Neverland Pendle SHMON',
    ...eventData(65, 91050, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.symbol, 'SHMON');
  assert.equal(reserve?.name, 'SHMON');
  assert.equal(reserve?.decimals, 8);

  // TokenInfo must be corrected too: config.ts seeds it from ReserveInitialized,
  // which carries no decimals, so it lands as 18 until this event fixes it.
  const tokenInfo = mockDb.entities.TokenInfo.get(ADDRESSES.asset);
  assert.equal(tokenInfo?.decimals, 8);
  assert.equal(tokenInfo?.symbol, 'SHMON');
});

test('aToken initialization propagates on-chain decimals into TokenInfo', async () => {
  // PoolConfigurator.ReserveInitialized does NOT carry decimals, so config.ts writes
  // TokenInfo with `getTokenMetadata(asset)?.decimals ?? 18`. For any asset absent from the
  // hardcoded table that is a blind 18. AToken.Initialized DOES carry the true
  // `aTokenDecimals`, so it must correct TokenInfo - otherwise TVL and LP valuation read 18
  // for a 6-decimal token, which is exactly what happened before XAUt0 was hand-added.
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: VIEM_ERROR_ADDRESS,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nMOCK',
    aTokenName: 'Neverland Interest Bearing MOCK',
    ...eventData(61, 91100, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const token = mockDb.entities.TokenInfo.get(VIEM_ERROR_ADDRESS);
  assert.equal(token?.decimals, 6, 'TokenInfo must carry the on-chain aTokenDecimals');
  assert.equal(token?.symbol, 'MOCK');

  const reserve = mockDb.entities.Reserve.get(`${VIEM_ERROR_ADDRESS}-${ADDRESSES.pool}`);
  assert.equal(reserve?.decimals, 6, 'Reserve and TokenInfo must agree on decimals');
});

test('reserve initialization keeps aToken-derived metadata for an unknown asset', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  // On chain, PoolConfigurator.initReserves initializes the aToken proxy BEFORE it
  // emits ReserveInitialized, so AToken.Initialized carries the lower log index and
  // config.ts writes last. For an asset absent from KNOWN_TOKENS, config.ts has only
  // the 'ERC20'/'Token ERC20'/18 placeholders to write, so it must not overwrite the
  // real values this event already derived from the aToken's own metadata.
  const aTokenInit = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'npAUSD',
    aTokenName: 'Neverland Pendle AUSD',
    ...eventData(70, 91100, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: aTokenInit, mockDb });

  const reserveInit = TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset: ADDRESSES.asset,
    aToken: ADDRESSES.aToken,
    stableDebtToken: '0x0000000000000000000000000000000000000000',
    variableDebtToken: ADDRESSES.variableToken,
    interestRateStrategyAddress: ADDRESSES.oracle,
    ...eventData(70, 91101, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: reserveInit,
    mockDb,
  });

  const reserve = mockDb.entities.Reserve.get(`${ADDRESSES.asset}-${ADDRESSES.pool}`);
  assert.equal(reserve?.decimals, 6);
  assert.equal(reserve?.symbol, 'AUSD');
  assert.equal(reserve?.name, 'AUSD');

  const tokenInfo = mockDb.entities.TokenInfo.get(ADDRESSES.asset);
  assert.equal(tokenInfo?.decimals, 6);
  assert.equal(tokenInfo?.symbol, 'AUSD');
});

test('aToken initialization prefers the curated table for a known asset', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  // WMON is in KNOWN_TOKENS, so the curated entry is taken whole. The event deliberately
  // carries a WRONG decimals here: a listed reserve must not be rewritten by on-chain
  // discovery, so all three fields come from the table and the 6n is ignored.
  const init = TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: WMON_ADDRESS,
    pool: ADDRESSES.pool,
    aTokenDecimals: 6n,
    aTokenSymbol: 'nWMON',
    aTokenName: 'Neverland Interest Bearing WMON',
    ...eventData(72, 91200, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Initialized.processEvent({ event: init, mockDb });

  const reserve = mockDb.entities.Reserve.get(`${WMON_ADDRESS}-${ADDRESSES.pool}`);
  assert.equal(reserve?.symbol, 'WMON');
  assert.equal(reserve?.name, 'Wrapped MON');
  assert.equal(reserve?.decimals, 18);

  const tokenInfo = mockDb.entities.TokenInfo.get(WMON_ADDRESS);
  assert.equal(tokenInfo?.name, 'Wrapped MON');
  assert.equal(tokenInfo?.decimals, 18);
});

// Every debt-token handler resolves its reserve through the SubToken row written by
// PoolConfigurator.ReserveInitialized. A token whose SubToken has not been indexed yet
// (dynamic registration is forward-only) must be a clean no-op rather than a partial write.
test('debt token events are no-ops without a SubToken row', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = TestHelpers.MockDb.createMockDb();

  const shared = { value: 100n, balanceIncrease: 0n, index: RAY };

  const vMint = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    ...shared,
    ...eventData(80, 92000, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: vMint, mockDb });

  const vBurn = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    ...shared,
    ...eventData(81, 92010, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: vBurn, mockDb });

  const vDelegated = TestHelpers.VariableDebtToken.BorrowAllowanceDelegated.createMockEvent({
    fromUser: ADDRESSES.fromUser,
    toUser: ADDRESSES.toUser,
    asset: ADDRESSES.asset,
    amount: 100n,
    ...eventData(85, 92050, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.BorrowAllowanceDelegated.processEvent({
    event: vDelegated,
    mockDb,
  });

  assert.deepEqual(mockDb.entities.UserReserve.getAll(), []);
  assert.deepEqual(mockDb.entities.Reserve.getAll(), []);
});

// SubToken wiring can land before the reserve and user rows exist (dynamic registration is
// forward-only, so a token's first indexed event may be a mid-life mint or burn). Burns of a
// position this indexer never saw opened must no-op, and mints must fall back to the event's
// own index rather than a reserve row that is not there yet.
test('debt token mints and burns tolerate missing reserve and user rows', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = TestHelpers.MockDb.createMockDb();

  for (const [id, underlying] of [
    [ADDRESSES.variableToken, ADDRESSES.asset],
    [ADDRESSES.stableToken, ADDRESSES.asset],
  ] as const) {
    mockDb = mockDb.entities.SubToken.set({
      id,
      pool_id: ADDRESSES.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: underlying,
      underlyingAssetDecimals: DECIMALS,
    });
  }

  const vBurn = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(90, 93000, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: vBurn, mockDb });

  // The burn may not invent a position.
  assert.deepEqual(mockDb.entities.UserReserve.getAll(), []);

  const vMint = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.fromUser,
    onBehalfOf: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(92, 93020, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: vMint, mockDb });

  const userReserve = mockDb.entities.UserReserve.get(
    `${ADDRESSES.fromUser}-${ADDRESSES.asset}-${ADDRESSES.pool}`
  );
  assert.ok(userReserve, 'a mint opens the position it is minting into');
});

// The repay path can run before the reserve row exists: the position is known (the user has
// debt) but PoolConfigurator.ReserveInitialized has not been indexed for this market yet.
// Index bookkeeping must then fall back to the event's own index, and the reserve-derived
// history and daily-highwater writes must be skipped instead of throwing.
test('debt token burns fall back to the event index when the reserve row is missing', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = TestHelpers.MockDb.createMockDb();

  for (const id of [ADDRESSES.variableToken, ADDRESSES.stableToken]) {
    mockDb = mockDb.entities.SubToken.set({
      id,
      pool_id: ADDRESSES.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: ADDRESSES.asset,
      underlyingAssetDecimals: DECIMALS,
    });
  }

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 1000n,
    currentDebt: 1000n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 100,
  });

  const vBurn = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(95, 94000, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: vBurn, mockDb });

  // No reserve row means no reserve-derived history rows.
  assert.deepEqual(mockDb.entities.ReserveParamsHistoryItem.getAll(), []);
  assert.equal(mockDb.entities.Reserve.get(reserveId), undefined);
  // The position itself still settles against the event's own index.
  assert.equal(mockDb.entities.UserReserve.get(userReserveId)?.variableBorrowIndex, RAY);
});

// A reserve that exists but has never accrued carries a zero variableBorrowIndex. Repay
// bookkeeping must fall back to the event's own index rather than writing that zero onto
// the user's position, which would make the next rayMul read the debt as zero.
test('debt token burns fall back to the event index when the reserve index is zero', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = TestHelpers.MockDb.createMockDb();

  for (const id of [ADDRESSES.variableToken, ADDRESSES.stableToken]) {
    mockDb = mockDb.entities.SubToken.set({
      id,
      pool_id: ADDRESSES.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: ADDRESSES.asset,
      underlyingAssetDecimals: DECIMALS,
    });
  }

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.fromUser}-${reserveId}`;
  const base = createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset);
  mockDb = mockDb.entities.Reserve.set({
    ...base,
    decimals: DECIMALS,
    liquidityIndex: 0n,
    variableBorrowIndex: 0n,
    liquidityRate: 0n,
    totalScaledDebt: 1000n,
    totalCurrentDebt: 1000n,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.fromUser,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 1000n,
    currentDebt: 1000n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 100,
  });

  const vBurn = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.fromUser,
    target: ADDRESSES.fromUser,
    value: 100n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(97, 95000, ADDRESSES.variableToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: vBurn, mockDb });

  assert.equal(mockDb.entities.UserReserve.get(userReserveId)?.variableBorrowIndex, RAY);
});
