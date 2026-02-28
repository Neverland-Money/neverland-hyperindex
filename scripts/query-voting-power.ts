#!/usr/bin/env tsx

const ENVIO_GRAPHQL_URL =
  process.env.ENVIO_GRAPHQL_URL || 'https://index.neverland.money/v1/graphql';
const HASURA_ADMIN_SECRET = process.env.HASURA_ADMIN_SECRET || 'H9bN8Q9waXiS';

interface UserLeaderboardState {
  id: string;
  user_id: string;
  votingPower: string;
  vpTierIndex: string | null;
  vpMultiplier: string;
  combinedMultiplier: string;
  nftCount: string;
  nftMultiplier: string;
  lastUpdate: number;
}

interface DustLockToken {
  id: string;
  owner: string;
  lockedAmount: string;
  end: number;
  isPermanent: boolean;
  updatedAt: number;
}

interface QueryResponse {
  UserLeaderboardState: UserLeaderboardState[];
  DustLockToken: DustLockToken[];
}

async function queryUserVotingPower(userAddress: string): Promise<void> {
  const normalizedAddress = userAddress.toLowerCase();

  const query = `
    query GetUserVotingPower($userId: String!) {
      UserLeaderboardState(where: { user_id: { _eq: $userId } }) {
        id
        user_id
        votingPower
        vpTierIndex
        vpMultiplier
        combinedMultiplier
        nftCount
        nftMultiplier
        lastUpdate
      }
      DustLockToken(where: { owner: { _eq: $userId } }) {
        id
        owner
        lockedAmount
        end
        isPermanent
        updatedAt
      }
    }
  `;

  try {
    const response = await fetch(ENVIO_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-hasura-admin-secret': HASURA_ADMIN_SECRET,
      },
      body: JSON.stringify({
        query,
        variables: { userId: normalizedAddress },
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result = await response.json();

    if (result.errors) {
      console.error('GraphQL Errors:', JSON.stringify(result.errors, null, 2));
      process.exit(1);
    }

    const data: QueryResponse = result.data;

    console.log('\n=== User Voting Power Data ===\n');
    console.log(`User Address: ${normalizedAddress}\n`);

    if (data.UserLeaderboardState && data.UserLeaderboardState.length > 0) {
      const state = data.UserLeaderboardState[0];
      console.log('Voting Power Summary:');
      console.log(`  Total Voting Power: ${state.votingPower}`);
      console.log(`  VP Tier Index: ${state.vpTierIndex || 'None'}`);
      console.log(`  VP Multiplier: ${state.vpMultiplier} (${Number(state.vpMultiplier) / 100}%)`);
      console.log(`  NFT Count: ${state.nftCount}`);
      console.log(`  NFT Multiplier: ${state.nftMultiplier} (${Number(state.nftMultiplier) / 100}%)`);
      console.log(`  Combined Multiplier: ${state.combinedMultiplier} (${Number(state.combinedMultiplier) / 100}%)`);
      console.log(`  Last Updated: ${new Date(state.lastUpdate * 1000).toISOString()}`);
    } else {
      console.log('No voting power data found for this user.');
    }

    if (data.DustLockToken.length > 0) {
      console.log(`\nDustLock Tokens (${data.DustLockToken.length}):`);
      data.DustLockToken.forEach((token, idx) => {
        console.log(`\n  Token #${idx + 1}:`);
        console.log(`    Token ID: ${token.id}`);
        console.log(`    Locked Amount: ${token.lockedAmount}`);
        console.log(`    Lock End: ${token.isPermanent ? 'PERMANENT' : new Date(token.end * 1000).toISOString()}`);
        console.log(`    Updated At: ${new Date(token.updatedAt * 1000).toISOString()}`);
      });
    } else {
      console.log('\nNo DustLock tokens found for this user.');
    }

    console.log('\n');
  } catch (error) {
    console.error('Error querying voting power:', error);
    process.exit(1);
  }
}

function main() {
  const args = process.argv.slice(2);

  let userAddress: string | undefined;

  for (let i = 0; i < args.length; i++) {
    if ((args[i] === '--user' || args[i] === '-u') && args[i + 1]) {
      userAddress = args[i + 1];
      break;
    }
  }

  if (!userAddress) {
    console.error('Error: --user argument is required');
    console.log('\nUsage: pnpm query:voting-power -- --user <address>');
    console.log('   or: pnpm query:voting-power -- -u <address>');
    process.exit(1);
  }

  queryUserVotingPower(userAddress);
}

main();
