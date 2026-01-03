# Neverland HyperIndex

HyperIndex for Neverland Protocol. This indexer tracks lending, rewards, NFT partnerships, leaderboards, and protocol configuration events to power analytics and product surfaces.

## Overview

The Neverland indexer provides real-time data aggregation for the Neverland DeFi protocol, including:

- **Lending Protocol**: Track supplies, borrows, interest rates, and reserve dynamics
- **Tokenomics**: Monitor DUST token emissions, veDUST voting escrow locks, and governance power
- **Rewards System**: Monitor reward distributions, emission schedules, and configuration history
- **NFT Partnerships**: Index partnership NFTs and their metadata
- **Leaderboard**: Track user points, rankings, and competitive metrics
- **Protocol Configuration**: Capture parameter updates and governance changes

## Architecture

Built with [Envio HyperIndex](https://www.envio.dev/), this indexer leverages:

- **HyperSync**: Up to 2000x faster blockchain data access
- **PostgreSQL**: Reliable data storage with full query capabilities
- **GraphQL API**: Real-time querying interface for frontend applications
- **TypeScript**: Type-safe handlers and generated bindings

## Requirements

- Node.js 22 or newer
- pnpm 10 or newer
- Docker Desktop (for local development only)

## Quickstart

### Local Development

```bash
# Install dependencies
pnpm install

# Start local dependencies (Postgres)
pnpm run local:docker:up

# Generate types and bindings
pnpm run codegen

# Start the indexer in development mode
pnpm run dev
```

### Environment Setup

Create a `.env` file in the project root (see `.env.example`):

```bash
# Required for HyperSync access (self-hosted only)
ENVIO_API_TOKEN=your-envio-api-token

# Optional: Custom RPC endpoint(s)
# RPC_URL_143=https://rpc-mainnet.monadinfra.com

# Optional: Logging
# LOG_LEVEL=debug
# METRICS_PORT=9090

# Optional: External read controls
# DISABLE_EXTERNAL_CALLS=true
# DISABLE_ETH_CALLS=true

# Optional: One-time chain baselines during settlement
# ENABLE_NFT_CHAIN_SYNC=true
# ENABLE_LP_CHAIN_SYNC=true

# Optional: LP debug logging
# DEBUG_LP_POINTS=true
```

> **Note**: For production deployments to Envio's hosted service, environment variables are configured through the Envio dashboard instead of a `.env` file.

## Common Scripts

### Development Commands

- `pnpm run codegen`: Generate Envio bindings and types from schema and config
- `pnpm run dev`: Run the local indexer with hot reload
- `pnpm run start`: Start the indexer in production mode
- `pnpm run stop`: Stop the running indexer
- `pnpm run local:docker:up`: Start local Postgres for development
- `pnpm run local:docker:down`: Stop local Postgres

### Code Quality

- `pnpm run format`: Format code with Prettier
- `pnpm run format:check`: Check code formatting
- `pnpm run lint`: Run ESLint checks
- `pnpm run lint:fix`: Fix ESLint issues
- `pnpm run type-check`: Perform TypeScript type checking

### Testing

- `pnpm run test:build`: Compile tests to `dist-test`
- `pnpm run test`: Compile and run unit tests
- `pnpm run test:coverage`: Generate coverage report
- `pnpm run test:coverage:check`: Enforce 100% test coverage

## Project Structure

```
neverland-hyperindex/
├── .github/                        # GitHub workflows and templates
├── .husky/                         # Git hooks
├── abis/                           # Contract ABIs
│   ├── helpers/                    # Utility contract ABIs
│   ├── leaderboard/                # Leaderboard contract ABIs
│   └── lending/                    # Lending protocol ABIs
├── src/
│   ├── __tests__/                  # Test suite (unit/integration/e2e)
│   ├── handlers/                   # Event handlers
│   │   ├── config.ts               # Protocol configuration events
│   │   ├── dustlock.ts             # Dust lock events
│   │   ├── leaderboard.ts          # Leaderboard and points events
│   │   ├── leaderboardKeeper.ts    # Leaderboard keeper/settlement events
│   │   ├── lp.ts                   # LP position tracking (Uniswap V3)
│   │   ├── nft.ts                  # NFT partnership events
│   │   ├── pool.ts                 # Lending protocol events
│   │   ├── rewards.ts              # Rewards distribution events
│   │   ├── shared.ts               # Shared handler utilities
│   │   └── tokenization.ts         # Tokenization events
│   ├── helpers/                    # Shared utilities
│   │   ├── constants.ts            # Constant values
│   │   ├── entityHelpers.ts        # Database entity helpers
│   │   ├── leaderboard.ts          # Leaderboard calculation logic
│   │   ├── math.ts                 # Mathematical operations (ray/wad)
│   │   ├── points.ts               # Points calculation logic
│   │   ├── protocolAggregation.ts  # Protocol-level aggregations
│   │   ├── uniswapV3.ts            # Uniswap V3 math helpers
│   │   └── viem.ts                 # Viem utilities
│   └── types/                      # TypeScript type definitions
│       └── shims.d.ts              # Type shims
├── .env.example                    # Environment variables template
├── .gitignore                      # Git ignore file
├── LICENSE                         # License file
├── README.md                       # Project documentation
├── config.yaml                     # Indexer configuration
├── package.json                    # Project dependencies and scripts
├── pnpm-workspace.yaml             # pnpm workspace configuration
├── schema.graphql                  # GraphQL schema definition
└── tsconfig.json                   # TypeScript configuration
```

## Configuration

### Network Configuration

The indexer is configured to track Neverland Protocol on Monad. Update `config.yaml` to:

- Add new contracts or events
- Adjust start blocks, batch sizes, and reorg depth
- Configure multiple networks (for multichain support)
- Override RPC URLs with `RPC_URL_<CHAIN_ID>` in `.env`

### Schema Updates

When modifying `schema.graphql`:

1. Run `pnpm run codegen` to regenerate types
2. Update handlers to use new entities
3. Run tests to ensure compatibility

## Deployment

### Envio Hosted Service (Recommended)

1. **Connect GitHub Repository**
   - Install the Envio Deployments GitHub App
   - Select your repository and configure deployment branch

2. **Configure Environment Variables**
   - Navigate to the Envio dashboard
   - Add required environment variables (prefixed with `ENVIO_`)
   - Optional performance flags live outside the `ENVIO_` namespace
   - No need for `ENVIO_API_TOKEN` - provided automatically

3. **Deploy**

   ```bash
   git push origin main  # Triggers automatic deployment
   ```

4. **Monitor**
   - View deployment status in the Envio Explorer
   - Check logs and sync status in real-time

### Self-Hosting

For self-hosted deployments, ensure:

```bash
# Set required environment variables
export ENVIO_API_TOKEN=your-token
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=envio
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

## Querying the Indexer

Once deployed, query your indexed data using GraphQL:

```graphql
query {
  ProtocolStats(where: { id: { _eq: "current" } }) {
    tvlUsd
    suppliesUsd
    borrowsUsd
    totalRevenueUsd
    updatedAt
  }
}

query {
  LeaderboardState {
    currentEpochNumber
    isActive
  }
  UserEpochStats(
    where: { epochNumber: { _eq: "1" } }
    orderBy: totalPoints
    orderDirection: desc
    first: 10
  ) {
    user_id
    totalPoints
    lpPoints
    depositPoints
    borrowPoints
    rank
    lastUpdatedAt
  }
  LeaderboardBlacklist(where: { isBlacklisted: { _eq: true } }) {
    user_id
    lastUpdate
  }
}

query {
  LPPoolConfig(where: { pool: { _eq: "0x..." } }) {
    pool
    token0
    token1
    fee
    lpRateBps
    isActive
  }
  LPPoolState(where: { pool: { _eq: "0x..." } }) {
    currentTick
    token0Price
    token1Price
    lastUpdate
  }
  LPPoolFeeStats(where: { pool: { _eq: "0x..." } }) {
    volumeUsd24h
    feesUsd24h
    feeAprBps
    lastUpdate
  }
  UserLPPosition(where: { user_id: { _eq: "0x..." } }) {
    tokenId
    pool
    isInRange
    valueUsd
    lastSettledAt
  }
}
```

## Monitoring and Debugging

### Local Development

- Access the GraphQL Playground at `http://localhost:4000/graphql`
- View database logs in the console
- Use `context.log()` in handlers for custom logging
- Enable `DEBUG_LP_POINTS=true` for verbose LP point tracing

### Production

- Monitor deployment health in the Envio dashboard
- Set up alerts for failures or performance issues
- Use IP whitelisting for additional security

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure all tests pass and coverage is at 100%
5. Submit a pull request

## Quality Gates

This project maintains high code quality standards:

- **Pre-commit hooks**: Enforce formatting, linting, and type checks
- **CI/CD**: Automated testing and deployment validation
- **Test Coverage**: 100% coverage requirement for all new code
- **Type Safety**: Strict TypeScript configuration

## Troubleshooting

### Common Issues

**Indexer not starting:**

- Check Node.js and pnpm versions
- Verify Docker is running (for local development)
- Ensure `.env` file is properly configured

**Missing data after deployment:**

- Verify the starting block number
- Check if contracts are correctly configured
- Review handler logic for data processing

**Performance issues:**

- Adjust `full_batch_size` in config.yaml
- Optimize handler logic
- Consider using unordered multichain mode

### Getting Help

- Join the [Envio Discord](https://discord.gg/envio) for community support
- Check the [Envio Documentation](https://docs.envio.dev/)
- Review existing issues in the repository

## License

See `LICENSE` for licensing information.
