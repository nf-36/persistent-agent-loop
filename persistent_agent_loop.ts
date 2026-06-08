import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import express, { Request, Response } from 'express';

dotenv.config();

/**
 * CONFIGURATION & TYPES
 */
const PORT = Number(process.env.PORT) || 3000;
const NEON_DATABASE_URL = process.env.DATABASE_URL;

interface AgentState {
  last_run_at: Date;
  iteration_count: number;
  memory_context: string;
  is_active: boolean;
}

interface StepOutcome {
  thought: string;
  action: 'SEARCH' | 'TRANSACT' | 'NOTIFY' | 'WAIT' | 'UPDATE_DB' | 'POLL_DEX';
  payload: Record<string, unknown>;
  confidence: number;
}

interface DexPair {
  chainId: string;
  dexId: string;
  pairAddress: string;
  baseToken: {
    name: string;
    symbol: string;
  };
  priceUsd: string;
  volume?: {
    h24: number;
  };
  liquidity?: {
    usd: number;
  };
  pairCreatedAt: number;
}

/**
 * DATABASE INITIALIZATION
 */
const pool = new Pool({
  connectionString: NEON_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function initDb() {
  try {
    const queryStr = "CREATE TABLE IF NOT EXISTS agent_logs (id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(), iteration INTEGER, thought TEXT, action VARCHAR(50), payload JSONB, status VARCHAR(20)); CREATE TABLE IF NOT EXISTS agent_state (id INT PRIMARY KEY DEFAULT 1, last_run_at TIMESTAMPTZ, iteration_count INTEGER DEFAULT 0, memory_context TEXT, is_active BOOLEAN DEFAULT TRUE, CONSTRAINT single_row CHECK (id = 1)); CREATE TABLE IF NOT EXISTS dex_pools (id SERIAL PRIMARY KEY, chain_id VARCHAR(50), dex_id VARCHAR(50), pair_address VARCHAR(255) UNIQUE, base_token_name VARCHAR(255), base_token_symbol VARCHAR(50), price_usd NUMERIC, volume_24h NUMERIC, liquidity_usd NUMERIC, pair_created_at TIMESTAMPTZ, last_updated TIMESTAMPTZ DEFAULT NOW()); INSERT INTO agent_state (id, iteration_count, is_active) VALUES (1, 0, TRUE) ON CONFLICT DO NOTHING;";
    await pool.query(queryStr);
    console.log('Database initialized.');
  } catch (err) {
    console.error('Error initializing database:', err);
  }
}

/**
 * DEX SCREENER INTEGRATION
 */
async function pollDexScreener(chains: string[]) {
  const results = [];
  for (const chainId of chains) {
    try {
      const url = "https://api.dexscreener.com/latest/dex/search/?q=" + chainId;
      const response = await (globalThis as unknown as { fetch: (u: string) => Promise<{ json: () => Promise<unknown> }> }).fetch(url);
      const data = await response.json() as { pairs?: DexPair[] };
      
      if (data && Array.isArray(data.pairs)) {
        const pairs = data.pairs.slice(0, 5);
        for (const pair of pairs) {
          try {
            const insertQuery = "INSERT INTO dex_pools (chain_id, dex_id, pair_address, base_token_name, base_token_symbol, price_usd, volume_24h, liquidity_usd, pair_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TO_TIMESTAMP($9 / 1000.0)) ON CONFLICT (pair_address) DO UPDATE SET price_usd = EXCLUDED.price_usd, volume_24h = EXCLUDED.volume_24h, liquidity_usd = EXCLUDED.liquidity_usd, last_updated = NOW();";
            await pool.query(insertQuery, [
              pair.chainId,
              pair.dexId,
              pair.pairAddress,
              pair.baseToken.name,
              pair.baseToken.symbol,
              pair.priceUsd,
              pair.volume?.h24,
              pair.liquidity?.usd,
              pair.pairCreatedAt
            ]);
            results.push({ symbol: pair.baseToken.symbol, chain: pair.chainId });
          } catch (dbErr) {
            console.error('Error inserting pair into DB:', dbErr);
          }
        }
      }
    } catch (err) {
      console.error("Error polling DEX Screener for chain " + chainId + ":", err);
    }
  }
  return results;
}

/**
 * CORE AGENT LOOP
 */
async function runAgentIteration() {
  const client = await pool.connect();
  try {
    const stateRes = await client.query('SELECT * FROM agent_state WHERE id = 1');
    const state: AgentState = stateRes.rows[0];

    if (!state.is_active) {
      console.log('Agent is currently inactive. Skipping iteration.');
      return;
    }

    console.log("Iteration " + state.iteration_count + " Starting autonomous reasoning...");

    // Autonomous reasoning logic (Simplified for external environment)
    const decision: StepOutcome = {
        thought: "Polling latest DEX pools to keep the database synchronized with Solana and Base trends.",
        action: "POLL_DEX",
        payload: { chains: ["solana", "base"] },
        confidence: 1.0
    };

    let status = 'SUCCESS';
    try {
      switch (decision.action) {
        case 'POLL_DEX':
          console.log('Polling DEX Screener...');
          const chains = (decision.payload.chains as string[]) || ['solana', 'base'];
          const polled = await pollDexScreener(chains);
          decision.payload.polled_summary = polled;
          break;
        case 'SEARCH':
          console.log('SEARCH action is currently restricted to the container environment.');
          break;
        case 'TRANSACT':
          console.log('Executing mock transaction:', decision.payload);
          break;
        case 'NOTIFY':
          console.log('Sending notification:', decision.payload.message);
          break;
        case 'WAIT':
          console.log('Agent decided to wait.');
          break;
        case 'UPDATE_DB':
          console.log('Updating knowledge base in Postgres.');
          break;
      }
    } catch (execError) {
      console.error('Execution Error:', execError);
      status = 'FAILED';
    }

    await client.query('BEGIN');
    await client.query(
      'UPDATE agent_state SET last_run_at = NOW(), iteration_count = iteration_count + 1, memory_context = $1 WHERE id = 1',
      [decision.thought.substring(0, 1000)]
    );
    await client.query(
      'INSERT INTO agent_logs (iteration, thought, action, payload, status) VALUES ($1, $2, $3, $4, $5)',
      [state.iteration_count, decision.thought, decision.action, decision.payload, status]
    );
    await client.query('COMMIT');

  } catch (error) {
    console.error('Critical Loop Error:', error);
    if (client) await client.query('ROLLBACK');
  } finally {
    if (client) client.release();
  }
}

async function main() {
  await initDb();
  
  const app = express();
  app.get('/', (req: Request, res: Response) => {
    res.send({ status: 'Agent Loop Running', timestamp: new Date() });
  });
  app.listen(PORT, '0.0.0.0', () => {
    console.log("Health check server listening on port " + PORT);
  });

  while (true) {
    try {
      await runAgentIteration();
    } catch (e) {
      console.error('Top-level loop crash, restarting in 30s...', e);
    }
    await new Promise(resolve => (globalThis as unknown as { setTimeout: (f: (v: unknown) => void, d: number) => void }).setTimeout(resolve, 60000));
  }
}

main().catch(console.error);