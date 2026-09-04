/**
 * Thin pass-through around envio's `indexer`, used by every handler module in place of
 * importing `indexer` from `'envio'` directly.
 *
 * It exists for one reason: a handful of tests drive a single handler with a context they
 * build themselves, which needs a way to look that handler up by contract and event. In v2
 * the generated code exposed a per-event register for exactly this. v3 keeps its
 * registrations in an internal global as opaque thunks and freezes `indexer`, so neither
 * reading nor wrapping it from the test side is possible.
 *
 * Recording is gated on the test tripwire, so a production run stores nothing and the calls
 * are a direct delegation.
 */
import { indexer as envioIndexer } from 'envio';

type HandlerArgs = { event: any; context: any };
type AnyHandler = (args: HandlerArgs) => Promise<void> | void;

const RECORD = process.env.NEVERLAND_TEST_ENV === '1';

const handlers = new Map<string, AnyHandler>();
const contractRegisters = new Map<string, AnyHandler>();

const key = (selector: { contract: string; event: string }) =>
  `${selector.contract}.${selector.event}`;

/** The registered handler for a contract/event, or undefined outside tests. */
export function lookupHandler(contract: string, event: string): AnyHandler | undefined {
  return handlers.get(`${contract}.${event}`);
}

/** The registered contractRegister callback for a contract/event, or undefined outside tests. */
export function lookupContractRegister(contract: string, event: string): AnyHandler | undefined {
  return contractRegisters.get(`${contract}.${event}`);
}

/**
 * Wraps a registration function so the handler is recorded, preserving the wrapped function's
 * own signature. The cast matters: typing these parameters loosely would erase envio's event
 * and context types at every call site, and with them the checks that catch, say, a bigint
 * field used as a number.
 */
function recording<T extends (selector: any, handler: any) => any>(
  register: T,
  store: Map<string, AnyHandler>
): T {
  return ((selector: { contract: string; event: string }, handler: AnyHandler) => {
    if (RECORD) store.set(key(selector), handler);
    return register(selector, handler);
  }) as T;
}

export const indexer = {
  onEvent: recording(envioIndexer.onEvent.bind(envioIndexer), handlers),
  contractRegister: recording(envioIndexer.contractRegister.bind(envioIndexer), contractRegisters),
};
