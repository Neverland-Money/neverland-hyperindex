const MAX_TICK = 887272;
const Q96 = 2n ** 96n;
const MAX_UINT256 = (1n << 256n) - 1n;

function getSqrtRatioAtTick(tick: number): bigint {
  if (tick > MAX_TICK || tick < -MAX_TICK) {
    throw new Error('TICK_OUT_OF_RANGE');
  }

  let absTick = tick < 0 ? BigInt(-tick) : BigInt(tick);
  let ratio =
    absTick & 0x1n ? 0xfffcb933bd6fad37aa2d162d1a594001n : 0x100000000000000000000000000000000n;

  if (absTick & 0x2n) ratio = (ratio * 0xfff97272373d413259a46990580e213an) >> 128n;
  if (absTick & 0x4n) ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdccn) >> 128n;
  if (absTick & 0x8n) ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0n) >> 128n;
  if (absTick & 0x10n) ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644n) >> 128n;
  if (absTick & 0x20n) ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0n) >> 128n;
  if (absTick & 0x40n) ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861n) >> 128n;
  if (absTick & 0x80n) ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053n) >> 128n;
  if (absTick & 0x100n) ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4n) >> 128n;
  if (absTick & 0x200n) ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54n) >> 128n;
  if (absTick & 0x400n) ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3n) >> 128n;
  if (absTick & 0x800n) ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9n) >> 128n;
  if (absTick & 0x1000n) ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825n) >> 128n;
  if (absTick & 0x2000n) ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5n) >> 128n;
  if (absTick & 0x4000n) ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7n) >> 128n;
  if (absTick & 0x8000n) ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6n) >> 128n;
  if (absTick & 0x10000n) ratio = (ratio * 0x9aa508b5b7a84e1c677de54f3e99bc9n) >> 128n;
  if (absTick & 0x20000n) ratio = (ratio * 0x5d6af8dedb81196699c329225ee604n) >> 128n;
  if (absTick & 0x40000n) ratio = (ratio * 0x2216e584f5fa1ea926041bedfe98n) >> 128n;
  if (absTick & 0x80000n) ratio = (ratio * 0x48a170391f7dc42444e8fa2n) >> 128n;

  if (tick > 0) {
    ratio = MAX_UINT256 / ratio;
  }

  const sqrtPriceX96 = (ratio >> 32n) + (ratio % (1n << 32n) === 0n ? 0n : 1n);
  return sqrtPriceX96;
}

function getAmount0ForLiquidity(
  sqrtRatioAX96: bigint,
  sqrtRatioBX96: bigint,
  liquidity: bigint
): bigint {
  let sqrtA = sqrtRatioAX96;
  let sqrtB = sqrtRatioBX96;
  if (sqrtA > sqrtB) {
    const temp = sqrtA;
    sqrtA = sqrtB;
    sqrtB = temp;
  }
  const numerator = liquidity << 96n;
  const diff = sqrtB - sqrtA;
  return (numerator * diff) / sqrtB / sqrtA;
}

function getAmount1ForLiquidity(
  sqrtRatioAX96: bigint,
  sqrtRatioBX96: bigint,
  liquidity: bigint
): bigint {
  let sqrtA = sqrtRatioAX96;
  let sqrtB = sqrtRatioBX96;
  if (sqrtA > sqrtB) {
    const temp = sqrtA;
    sqrtA = sqrtB;
    sqrtB = temp;
  }
  const diff = sqrtB - sqrtA;
  return (liquidity * diff) / Q96;
}

export function getAmountsForLiquidity(
  sqrtPriceX96: bigint,
  tickLower: number,
  tickUpper: number,
  liquidity: bigint
): { amount0: bigint; amount1: bigint } {
  const sqrtRatioAX96 = getSqrtRatioAtTick(tickLower);
  const sqrtRatioBX96 = getSqrtRatioAtTick(tickUpper);

  if (sqrtPriceX96 <= sqrtRatioAX96) {
    return {
      amount0: getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity),
      amount1: 0n,
    };
  }

  if (sqrtPriceX96 < sqrtRatioBX96) {
    return {
      amount0: getAmount0ForLiquidity(sqrtPriceX96, sqrtRatioBX96, liquidity),
      amount1: getAmount1ForLiquidity(sqrtRatioAX96, sqrtPriceX96, liquidity),
    };
  }

  return {
    amount0: 0n,
    amount1: getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity),
  };
}
