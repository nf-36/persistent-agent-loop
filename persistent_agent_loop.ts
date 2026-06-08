import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import express, { Request, Response } from 'express';

dotenv.config();

/**
 * CONFIGURATION & TYPES
 */
const PORT = Number(process.env.PORT) || 3000;
const NEON_DATABASE_URL = process.env.DATABASE_URL;

if (!NEON_DATABASE_URL) {
  console.error("FATAL: DATABASE_URL is not defined in environment variables.");
  process.exit(1);
}

// Optimization thresholds
const MIN_VOLUME_THRESHOLD = 500000; // Only write high-volume pairs to DB
const MIN_LIQUIDITY_THRESHOLD = 10000;
const ALERT_CACHE_TTL = 300000; // 5 minutes in-memory cache for alerts

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
 * IN-MEMORY STATE & CACHING (Saves DB Compute)
 */
let cachedAlerts: unknown[] = [];
let lastAlertFetchTime = 0;
let localIterationCount = 0;

/**
 * DATABASE INITIALIZATION
 */
const pool = new Pool({
  connectionString: NEON_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
  // Aggressive timeout settings to close idle connections quickly
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
});

async function initDb() {
  try {
    const queryStr = "CREATE TABLE IF NOT EXISTS agent_logs (id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(), iteration INTEGER, thought TEXT, action VARCHAR(50), payload JSONB, status VARCHAR(20)); CREATE TABLE IF NOT EXISTS agent_state (id INT PRIMARY KEY DEFAULT 1, last_run_at TIMESTAMPTZ, iteration_count INTEGER DEFAULT 0, memory_context TEXT, is_active BOOLEAN DEFAULT TRUE, CONSTRAINT single_row CHECK (id = 1)); CREATE TABLE IF NOT EXISTS dex_pools (id SERIAL PRIMARY KEY, chain_id VARCHAR(50), dex_id VARCHAR(50), pair_address VARCHAR(255) UNIQUE, base_token_name VARCHAR(255), base_token_symbol VARCHAR(50), price_usd NUMERIC, volume_24h NUMERIC, liquidity_usd NUMERIC, pair_created_at TIMESTAMPTZ, last_updated TIMESTAMPTZ DEFAULT NOW()); INSERT INTO agent_state (id, iteration_count, is_active) VALUES (1, 0, TRUE) ON CONFLICT DO NOTHING;";
    await pool.query(queryStr);
    console.log('Database initialized.');
  } catch (err) {
    console.error('Error initializing database:', err);
    process.exit(1);
  }
}

/**
 * DEX SCREENER INTEGRATION - Optimized with Filter-Before-Write
 */
async function pollDexScreener(chains: string[]) {
  const anomalies = [];
  for (const chainId of chains) {
    try {
      const url = "https://api.dexscreener.com/latest/dex/search/?q=" + chainId;
      const response = await (globalThis as unknown as { fetch: (u: string) => Promise<{ json: () => Promise<unknown> }> }).fetch(url);
      const data = await response.json() as { pairs?: DexPair[] };
      
      if (data && Array.isArray(data.pairs)) {
        // Only process high-value pairs to minimize DB writes
        const highValuePairs = data.pairs.filter(p => 
          (p.volume?.h24 ?? 0) > MIN_VOLUME_THRESHOLD && 
          (p.liquidity?.usd ?? 0) > MIN_LIQUIDITY_THRESHOLD
        );

        for (const pair of highValuePairs) {
          try {
            const insertQuery = "INSERT INTO dex_pools (chain_id, dex_id, pair_address, base_token_name, base_token_symbol, price_usd, volume_24h, liquidity_usd, pair_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TO_TIMESTAMP($9 / 1000.0)) ON CONFLICT (pair_address) DO UPDATE SET price_usd = EXCLUDED.price_usd, volume_24h = EXCLUDED.volume_24h, liquidity_usd = EXCLUDED.liquidity_usd, last_updated = NOW() WHERE dex_pools.price_usd != EXCLUDED.price_usd OR dex_pools.volume_24h < EXCLUDED.volume_24h;";
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
            anomalies.push({ symbol: pair.baseToken.symbol, chain: pair.chainId, vol: pair.volume?.h24 });
          } catch (dbErr) {
            console.error('Error inserting pair into DB:', dbErr);
          }
        }
      }
    } catch (err) {
      console.error("Error polling DEX Screener for chain " + chainId + ":", err);
    }
  }
  // Update local cache whenever new anomalies are found
  if (anomalies.length > 0) {
    lastAlertFetchTime = 0; // Invalidate cache to force fresh DB fetch on next /alerts request
  }
  return anomalies;
}

/**
 * CORE AGENT LOOP - Optimized to reduce DB overhead
 */
async function runAgentIteration() {
  localIterationCount++;
  
  // Every 10 iterations, we sync status to DB, otherwise keep it in memory
  const shouldSyncState = localIterationCount % 10 === 0;

  try {
    console.log("Iteration " + localIterationCount + " Starting (Internal Monologue)...");

    const decision: StepOutcome = {
        thought: "Monitoring DEX for high-volume plays. Filter-before-write logic active to save DB compute.",
        action: "POLL_DEX",
        payload: { chains: ["solana", "base"] },
        confidence: 1.0
    };

    if (decision.action === 'POLL_DEX') {
      const chains = (decision.payload.chains as string[]) || ['solana', 'base'];
      await pollDexScreener(chains);
    }

    // Only hit the DB to log state every 10 runs to save compute hours
    if (shouldSyncState) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(
          'UPDATE agent_state SET last_run_at = NOW(), iteration_count = iteration_count + $1 WHERE id = 1',
          [10]
        );
        await client.query(
          'INSERT INTO agent_logs (iteration, thought, action, payload, status) VALUES ($1, $2, $3, $4, $5)',
          [localIterationCount, decision.thought, decision.action, decision.payload, 'SUCCESS']
        );
        await client.query('COMMIT');
        console.log("State synced to Neon.");
      } finally {
        client.release();
      }
    }

  } catch (error) {
    console.error('Iteration Error:', error);
  }
}

async function main() {
  await initDb();
  
  const app = express();
  app.get('/', (req: Request, res: Response) => {
    res.send({ status: 'Agent Loop Running (Optimized)', iteration: localIterationCount });
  });

  // Alert endpoint with Cache-Aside logic
  app.get('/alerts', async (req: Request, res: Response) => {
    const now = Date.now();
    if (cachedAlerts.length > 0 && (now - lastAlertFetchTime) < ALERT_CACHE_TTL) {
      return res.json({ alerts: cachedAlerts, source: 'cache', timestamp: new Date() });
    }

    try {
      const alertQuery = `
        SELECT * FROM dex_pools 
        WHERE volume_24h > $1 
        AND liquidity_usd > $2
        ORDER BY volume_24h DESC 
        LIMIT 10
      `;
      const result = await pool.query(alertQuery, [MIN_VOLUME_THRESHOLD, MIN_LIQUIDITY_THRESHOLD]);
      cachedAlerts = result.rows;
      lastAlertFetchTime = now;
      res.json({ alerts: cachedAlerts, source: 'database', timestamp: new Date() });
    } catch (err) {
      console.error('Error fetching alerts:', err);
      res.status(500).json({ error: 'Failed to fetch alerts' });
    }
  });

  app.listen(PORT, '0.0.0.0', () => {
    console.log("Optimized Agent listening on port " + PORT);
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
