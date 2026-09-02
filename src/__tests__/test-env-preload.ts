/**
 * Runner-level test environment, loaded via `--import` ahead of every test module so it
 * applies regardless of which test file imports what. Thirteen of the suite's test files do
 * not import `v3-test-helpers`, so a seam-level scrub cannot cover them.
 *
 * It ASSIGNS rather than deletes. `delete` removes the own-property, and dotenv only skips
 * keys already present on `process.env` (dotenv@16.4.5 lib/main.js:324-338), so a deleted key
 * is repopulated from the repo `.env` as soon as `envio` runs `import 'dotenv/config'`
 * (envio/src/Env.res.mjs:11). An assigned value survives that import, because override is
 * reachable only through `DOTENV_CONFIG_OVERRIDE` (lib/env-options.js:16-18) or a
 * `dotenv_config_override=` argv token (lib/cli-options.js:1), and this repo sets neither.
 * Assignment is therefore order-independent; deletion is not.
 *
 * Deliberately dependency-free: something whose whole job is to run first should not drag in
 * module graphs that might themselves read the environment. The sentinel below is duplicated
 * from `helpers/prefill.ts` for that reason, and `test-env-guard.test.ts` pins the two equal.
 */

// Prefill is an operator setting, not a test input. Tests needing it opt in per-case and
// restore this value afterwards.
process.env.PREFILL_HISTORIC_EPOCHS = 'false';

// Must match TEST_PREFILL_DIR_SENTINEL in src/helpers/prefill.ts.
process.env.PREFILL_DATA_DIR = '__NEVERLAND_TEST_PREFILL_DIR_MUST_BE_OVERRIDDEN__';

// Arms the tripwire in helpers/prefill.ts. Production never sets this.
process.env.NEVERLAND_TEST_ENV = '1';
