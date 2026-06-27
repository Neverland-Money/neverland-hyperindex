---
name: isolated-pendle-loop-apy
description: Isolated Pendle-AUSD fixed-yield loop projection math contract (netLoopApy + PT fixed-yield decoupling)
metadata:
  type: project
---

Isolated Markets (first vault: Pendle AUSD) loop projection in `utils/isolated-markets.ts`.

- `netLoopApy(L, ptFixedYieldPct, ausdBorrowApyPct) = ptFixedYieldPct*L - ausdBorrowApyPct*(L-1)`. Dimensionally correct: on equity E, yield on exposure E*L minus borrow cost on debt E*(L-1), as a % of E. Verified test baselines: 24.0% @3x, 18.2% @2x, 12.4% @1x (`tests/isolated-markets.test.ts`).
- PT is COLLATERAL-ONLY / non-borrowable → Aave `supplyAPY ≈ 0` by construction. The loop yield is the Pendle implied (discount-to-par) APY, a SEPARATE number sourced from `meta.defaults.ptFixedYieldPct` (launch fallback 12.4) with TODO(pendle-data) seam for live binding. NEVER use `ptReserve.supplyAPY` as the loop yield — it would zero/negate the headline. `ptSupplyApyPct` (the ~0 lending APY) is computed in `useIsolatedVault` but currently rendered nowhere.
- Drift watch: net-APY is shown unconditionally in GREEN; if live `ausdBorrowApyPct` (live from `ausdReserve.variableBorrowAPY`) exceeds the 12.4 config fixed yield, projection goes negative-in-green (no clamp). Real but low-sev on a launch/preview surface.
- HF/liq math: `loopLtv=(L-1)/L`, `loopHealthFactor=liqLtv/loopLtv`, `loopLiqPrice=ptPrice*loopLtv/liqLtv`, `loopBuffer=(ptPrice-liq)/ptPrice` clamped. Risk envelope `PENDLE_AUSD_RISK` liqLtv 0.935 / maxLtv 0.915; 10x slider max → loopLtv 0.9 < maxLtv (openable). No isolated/pendle guardrail entry exists in `agents/guardrails.md` (new feature, not a math-invariant change).
