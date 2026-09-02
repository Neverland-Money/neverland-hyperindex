// Standalone probe: CommonJS output has no top-level await, so the body runs in an
// async IIFE. `export {}` keeps the file a module.
export {};

void (async () => {
  const query = process.env.PARITY_LIVE_MOCK_PROBE_QUERY;
  const url = process.env.PROD_GRAPHQL_URL;

  if (!query || !url) throw new Error('mock probe query and URL are required');

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
    process.stdout.write(await response.text());
  } catch (error) {
    console.error('mock probe failed:', error instanceof Error ? error.message : error);
    process.exitCode = 2;
  }
})();
