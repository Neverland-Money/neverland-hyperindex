---
name: frontend-preview-reserve-apy
description: How frontendPreview reserves flow through the apy-component-model and APYBadge
metadata:
  type: project
---

Pendle-AUSD preview reserves (`utils/pendle-ausd-preview-reserve.ts`) are injected only when `isPendleAusdFrontendPreviewEnabled()` (env flag NEXT_PUBLIC_MOCK_PENDLE_AUSD_MARKET / _PREVIEW_), never in prod.

- `resolveSupplyApyComponents` / `resolveBorrowApyComponents` (`utils/apy-component-model.ts`) detect preview reserves via `isFrontendPreviewReserve` (checks `frontendPreview` marker) and force `externalEntries = []` / `externalApyDecimal = 0`, but STILL read `supplyAPY`/`variableBorrowAPY` as the base component. So preview reserve APY contribution = its raw `supplyAPY`.
- Mock PT reserve `supplyAPY` set to `'0'` (was 0.0598) so the canonical markets `APYBadge` returns null (clean teaser) — `APYBadge` (`components/shared/apy-badge.tsx`) returns null when `displayValue===0 && !hasAnyExternalAPY`. PT symbol `PT-AUSD-8OCT2026` is NOT an LST and has NO external-APY registry entry, so badge correctly hides. Faithful to collateral-only-PT-earns-~0.
