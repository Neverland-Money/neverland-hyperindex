import { readFileSync } from 'node:fs';

type MockEndpointRows = Record<string, unknown>;

type LiveMockFixture = {
  baseline: MockEndpointRows;
  candidate: MockEndpointRows;
};

const fixtureRaw = process.env.PARITY_LIVE_MOCK_PATH
  ? readFileSync(process.env.PARITY_LIVE_MOCK_PATH, 'utf8')
  : process.env.PARITY_LIVE_MOCK_JSON;
if (!fixtureRaw) throw new Error('PARITY_LIVE_MOCK_JSON or PARITY_LIVE_MOCK_PATH is required');
const fixture = JSON.parse(fixtureRaw) as LiveMockFixture;

const mockFieldManifestRaw = process.env.PARITY_LIVE_MOCK_FIELD_MANIFEST;
if (!mockFieldManifestRaw) throw new Error('PARITY_LIVE_MOCK_FIELD_MANIFEST is required');
const PAGINATED_FIELDS = JSON.parse(mockFieldManifestRaw) as Record<string, readonly string[]>;

function requestUrl(input: string | URL | Request): string {
  return typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
}

function jsonResponse(entity: string, rows: unknown): Response {
  return new Response(JSON.stringify({ data: { [entity]: rows } }), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

type ParsedMockQuery = {
  entity: string;
  argumentsText: string;
  fields: string[];
};

function parseMockQuery(query: string): ParsedMockQuery {
  const match = query.match(
    /^\s*query\s*\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(([\s\S]*)\)\s*\{\s*([^{}]+?)\s*\}\s*\}\s*$/
  );
  if (!match) throw new Error(`unsupported GraphQL query shape: ${query}`);
  return {
    entity: match[1],
    argumentsText: match[2].trim(),
    fields: match[3].trim().split(/\s+/),
  };
}

function requireExactFields(
  entity: string,
  selected: string[],
  expected: readonly string[]
): string[] {
  const selectedSet = new Set(selected);
  const expectedSet = new Set(expected);
  if (
    selectedSet.size !== selected.length ||
    selected.length !== expected.length ||
    selected.some(field => !expectedSet.has(field))
  ) {
    throw new Error(
      `${entity} field selection must exactly match [${expected.join(', ')}], received [${selected.join(', ')}]`
    );
  }
  return selected;
}

function requireExactArguments(entity: string, actual: string, expected: RegExp): RegExpMatchArray {
  const match = actual.match(expected);
  if (!match) throw new Error(`${entity} has an unsupported GraphQL query shape`);
  return match;
}

function projectRows(
  entity: string,
  rows: readonly unknown[],
  fields: readonly string[]
): unknown[] {
  return rows.map((row, index) => {
    if (typeof row !== 'object' || row === null || Array.isArray(row)) {
      throw new Error(`${entity} mock row ${index} must be an object`);
    }
    const record = row as Record<string, unknown>;
    const projected: Record<string, unknown> = {};
    for (const field of fields) {
      if (!Object.prototype.hasOwnProperty.call(record, field)) {
        throw new Error(`${entity} mock row ${index} is missing selected field ${field}`);
      }
      projected[field] = record[field];
    }
    return projected;
  });
}

function sourceRows(endpointRows: MockEndpointRows, entity: string): unknown[] {
  const rows = endpointRows[entity] ?? [];
  if (!Array.isArray(rows)) throw new Error(`${entity} mock fixture must be an array`);
  return rows;
}

globalThis.fetch = async (input, init) => {
  const fetchRequest = new Request(input, init);
  const url = requestUrl(input);
  const endpointRows =
    url === process.env.PROD_GRAPHQL_URL
      ? fixture.baseline
      : url === process.env.LOCAL_GRAPHQL_URL
        ? fixture.candidate
        : undefined;
  if (!endpointRows) throw new Error(`unexpected live-adapter URL ${url}`);
  if (typeof init?.body !== 'string') throw new Error('expected a JSON GraphQL request body');
  const request = JSON.parse(init.body) as { query?: unknown };
  if (typeof request.query !== 'string') throw new Error('expected a GraphQL query string');

  const query = request.query;
  const parsedQuery = parseMockQuery(query);
  const { entity, argumentsText, fields: selectedFields } = parsedQuery;
  const fault = process.env.PARITY_LIVE_MOCK_FAULT;
  const faultEntity = process.env.PARITY_LIVE_MOCK_FAULT_ENTITY || 'LeaderboardEpoch';
  const faultEndpoint = process.env.PARITY_LIVE_MOCK_FAULT_ENDPOINT || 'candidate';
  const endpointName = endpointRows === fixture.baseline ? 'baseline' : 'candidate';
  const expectedSecret =
    endpointName === 'baseline'
      ? process.env.PARITY_LIVE_MOCK_EXPECTED_PROD_SECRET
      : process.env.PARITY_LIVE_MOCK_EXPECTED_CANDIDATE_SECRET;
  if (expectedSecret && fetchRequest.headers.get('x-hasura-admin-secret') !== expectedSecret) {
    throw new Error(`${endpointName} admin-secret header mismatch`);
  }
  const applyFault = Boolean(fault && entity === faultEntity && endpointName === faultEndpoint);

  if (applyFault && fault === 'never-resolve') {
    return new Promise<Response>((_resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('mock fetch remained unresolved')), 60_000);
      init.signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          reject(init.signal?.reason ?? new Error('mock fetch aborted'));
        },
        { once: true }
      );
    });
  }
  if (applyFault && fault === 'echo-secret') {
    throw new Error(
      `mock transport echoed ${fetchRequest.headers.get('x-hasura-admin-secret') ?? 'missing header'}`
    );
  }
  if (applyFault && fault === 'graphql-error-secret') {
    return new Response(
      JSON.stringify({
        errors: [
          {
            message: `mock GraphQL echoed ${fetchRequest.headers.get('x-hasura-admin-secret') ?? 'missing header'}`,
          },
        ],
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  if (entity === 'chain_metadata') {
    requireExactArguments(
      entity,
      argumentsText,
      /^where:\s*\{\s*chain_id:\s*\{\s*_eq:\s*143\s*\}\s*\}\s*,\s*limit:\s*1$/
    );
    const fields = requireExactFields(entity, selectedFields, [
      'chain_id',
      'latest_processed_block',
    ]);
    return jsonResponse(
      entity,
      projectRows(entity, sourceRows(endpointRows, entity).slice(0, 1), fields)
    );
  }

  if (entity === 'UserLeaderboardState' && /\blastUpdate\b/.test(argumentsText)) {
    requireExactArguments(
      entity,
      argumentsText,
      /^order_by:\s*\{\s*lastUpdate:\s*desc\s*\}\s*,\s*limit:\s*1$/
    );
    const fields = requireExactFields(entity, selectedFields, ['lastUpdate']);
    const rows = sourceRows(endpointRows, entity)
      .slice()
      .sort(
        (left, right) =>
          Number((right as { lastUpdate?: unknown }).lastUpdate ?? 0) -
          Number((left as { lastUpdate?: unknown }).lastUpdate ?? 0)
      )
      .slice(0, 1);
    return jsonResponse(entity, projectRows(entity, rows, fields));
  }

  const expectedFields = PAGINATED_FIELDS[entity];
  if (!expectedFields) throw new Error(`unexpected GraphQL entity ${entity}`);
  const keeperPredicate =
    entity === 'LeaderboardKeeperUserSettled' ? ',\\s*isGap:\\s*\\{\\s*_eq:\\s*false\\s*\\}' : '';
  const argumentPattern = new RegExp(
    `^where:\\s*\\{\\s*id:\\s*\\{\\s*_gt:\\s*("(?:\\\\.|[^"\\\\])*")\\s*\\}${keeperPredicate}\\s*\\}\\s*,\\s*order_by:\\s*\\{\\s*id:\\s*asc\\s*\\}\\s*,\\s*limit:\\s*([0-9]+)$`
  );
  const argumentMatch = requireExactArguments(entity, argumentsText, argumentPattern);
  const fields = requireExactFields(entity, selectedFields, expectedFields);
  const cursorRaw = argumentMatch[1];
  const limitRaw = argumentMatch[2];
  const limit = limitRaw ? Number(limitRaw) : 0;
  if (!Number.isInteger(limit) || limit < 1 || limit > 1_000) {
    throw new Error(`${entity} query must use a positive limit no larger than 1000`);
  }
  const requestedCursor = JSON.parse(cursorRaw) as unknown;
  if (typeof requestedCursor !== 'string') throw new Error(`${entity} cursor must be a string`);

  if (applyFault && fault === 'non-array') return jsonResponse(entity, { invalid: true });

  const effectiveCursor = applyFault && fault === 'repeat-first-page' ? '' : requestedCursor;
  let rows = sourceRows(endpointRows, entity)
    .filter(row => {
      if (typeof row !== 'object' || row === null || Array.isArray(row)) return false;
      const record = row as Record<string, unknown>;
      if (entity === 'LeaderboardKeeperUserSettled' && record.isGap !== false) return false;
      return typeof record.id === 'string' && record.id > effectiveCursor;
    })
    // Code-unit order: the same rule as the `>` cursor filter above, as fetchAll's
    // strictly-increasing validator, and as the byte-ordered Postgres behind the real endpoint.
    // localeCompare disagrees on case and on separator-vs-digit ids.
    .sort((left, right) => {
      const l = String((left as { id: unknown }).id);
      const r = String((right as { id: unknown }).id);
      return l < r ? -1 : l > r ? 1 : 0;
    })
    .slice(0, applyFault && fault === 'oversized' ? limit + 1 : limit);
  rows = projectRows(entity, rows, fields);

  if (applyFault) {
    if (fault === 'descending') rows.reverse();
    if (fault === 'duplicate-id' && rows.length >= 2) {
      (rows[1] as Record<string, unknown>).id = (rows[0] as Record<string, unknown>).id;
    }
    if (fault === 'missing-id' && rows.length) delete (rows[0] as Record<string, unknown>).id;
    if (fault === 'non-string-id' && rows.length) (rows[0] as Record<string, unknown>).id = 8;
  }

  return jsonResponse(entity, rows);
};
