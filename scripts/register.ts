#!/usr/bin/env npx tsx
/**
 * OCR Worker Registration Script
 *
 * Automated registration flow using @arke-institute/rhiza registration module:
 * - Creates new klados with verification and API key
 * - Updates existing klados, re-verifying if endpoint changes
 * - Supports dry-run mode to preview changes
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts              # Test network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts --production # Main network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts --dry-run    # Preview only
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';
import { ArkeClient } from '@arke-institute/sdk';
import {
  syncKlados,
  readState,
  writeState,
  getStateFilePath,
  findWorkspaceConfig,
  resolveWorkspaceCollection,
  migrateKladosToCollection,
  type KladosConfig,
  type KladosRegistrationState,
  type DryRunResult,
  type SyncResult,
} from '@arke-institute/rhiza/registration';
import { CloudflareKeyStore } from './cloudflare-keystore.js';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;

// =============================================================================
// Helper Functions
// =============================================================================

async function waitForDeployment(endpoint: string, maxWaitMs = 30000): Promise<void> {
  const startTime = Date.now();
  const checkInterval = 2000;

  console.log(`  Waiting for ${endpoint}/health...`);

  while (Date.now() - startTime < maxWaitMs) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const res = await fetch(`${endpoint}/health`, {
        method: 'GET',
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      if (res.ok) {
        console.log('  Worker is responding');
        return;
      }
    } catch {
      // Ignore errors, keep trying
    }
    await new Promise((resolve) => setTimeout(resolve, checkInterval));
  }

  console.warn('  Health check timed out, attempting verification anyway...');
}

function updateWranglerConfig(kladosId: string): boolean {
  try {
    const wranglerPath = 'wrangler.jsonc';
    if (!existsSync(wranglerPath)) return false;

    let content = readFileSync(wranglerPath, 'utf-8');
    // Replace AGENT_ID placeholder or existing value
    content = content.replace(/"AGENT_ID":\s*"[^"]*"/, `"AGENT_ID": "${kladosId}"`);
    writeFileSync(wranglerPath, content);
    return true;
  } catch {
    return false;
  }
}

function isDryRunResult(
  result: SyncResult<KladosRegistrationState> | DryRunResult
): result is DryRunResult {
  return (
    result.action === 'would_create' ||
    result.action === 'would_update' ||
    (result.action === 'unchanged' && !('state' in result))
  );
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  const isProduction =
    process.argv.includes('--production') || process.argv.includes('--prod');
  const isDryRun = process.argv.includes('--dry-run');
  const migrateCollection = process.argv.includes('--migrate-collection');
  const network = isProduction ? 'main' : 'test';

  console.log(`\n📦 OCR Worker Registration (${network} network)${isDryRun ? ' [DRY RUN]' : ''}${migrateCollection ? ' [MIGRATE]' : ''}\n`);

  // Load agent config
  if (!existsSync('agent.json')) {
    console.error('Error: agent.json not found');
    process.exit(1);
  }

  const config: KladosConfig = JSON.parse(readFileSync('agent.json', 'utf-8'));
  console.log(`Agent: ${config.label}`);
  console.log(`Endpoint: ${config.endpoint}`);
  console.log('');

  // Load existing state
  const stateFile = getStateFilePath('.klados-state', network);
  const state = readState<KladosRegistrationState>(stateFile);

  if (state) {
    console.log(`Found existing klados: ${state.klados_id}`);
  } else {
    console.log('Creating new klados...\n');
  }

  // Create client
  const client = new ArkeClient({ authToken: ARKE_USER_KEY, network });

  // Create key store
  const keyStore = new CloudflareKeyStore(process.cwd());

  // Check for workspace config (shared collection across kladoi)
  const workspace = findWorkspaceConfig();
  let collectionId: string | undefined;
  let updatedState = state;

  if (workspace) {
    console.log(`Found workspace config: ${workspace.path}`);
    if (!isDryRun) {
      const resolved = await resolveWorkspaceCollection(client, network, workspace.path);
      collectionId = resolved.collectionId;
      if (!resolved.created) {
        console.log(`Using workspace collection: ${collectionId}`);
      }

      // Handle --migrate-collection: move existing klados to workspace collection
      if (migrateCollection && state && state.collection_id !== collectionId) {
        console.log(`\nMigrating klados from ${state.collection_id} to ${collectionId}...`);
        await migrateKladosToCollection(client, state.klados_id, state.collection_id, collectionId);
        updatedState = { ...state, collection_id: collectionId, updated_at: new Date().toISOString() };
        writeState(stateFile, updatedState);
        console.log(`  Updated local state`);
      } else if (migrateCollection && state && state.collection_id === collectionId) {
        console.log(`Klados already in workspace collection`);
      }
    } else {
      const networkConfig = workspace.config[network];
      if (networkConfig.collection_id) {
        collectionId = networkConfig.collection_id;
        console.log(`Would use workspace collection: ${collectionId}`);
        if (migrateCollection && state && state.collection_id !== collectionId) {
          console.log(`Would migrate klados from ${state.collection_id} to ${collectionId}`);
        }
      } else {
        console.log(`Would create workspace collection: ${networkConfig.collection_label}`);
      }
    }
    console.log('');
  } else if (migrateCollection) {
    console.warn('Warning: --migrate-collection requires a workspace config (.arke-workspace.json)');
    console.log('');
  }

  try {
    // Sync klados
    const result = await syncKlados(client, config, updatedState, {
      network,
      collectionId,
      keyStore,
      dryRun: isDryRun,
      onDeploy: async () => {
        console.log('\n🚀 Deploying worker...');
        execSync('wrangler deploy', { stdio: 'inherit' });
      },
      onWaitForHealth: async (endpoint) => {
        console.log('\n⏳ Waiting for deployment...');
        await waitForDeployment(endpoint);
      },
    });

    // Handle dry run result
    if (isDryRunResult(result)) {
      console.log(`\n📋 Would: ${result.action}`);
      if (result.changes && result.changes.length > 0) {
        console.log('\nChanges:');
        for (const change of result.changes) {
          console.log(`  ${change.field}: ${change.from ?? '(none)'} → ${change.to}`);
        }
      }
      console.log('\nRun without --dry-run to apply changes.');
      return;
    }

    // Handle actual sync result
    const { action, state: newState } = result;

    if (action === 'created') {
      // Update wrangler.jsonc with AGENT_ID
      console.log('\n📝 Updating wrangler.jsonc...');
      if (updateWranglerConfig(newState.klados_id)) {
        console.log(`  AGENT_ID set to ${newState.klados_id}`);
      } else {
        console.warn('  Could not update wrangler.jsonc');
        console.warn(`  Set AGENT_ID manually: "${newState.klados_id}"`);
      }

      // Final deploy with correct AGENT_ID
      console.log('\n🚀 Final deployment...');
      execSync('wrangler deploy', { stdio: 'inherit' });
    }

    // Save state
    if (action !== 'unchanged') {
      writeState(stateFile, newState);
    }

    // Print result
    console.log(`\n${'='.repeat(60)}`);
    console.log(`✅ Klados ${action}!`);
    console.log(`${'='.repeat(60)}`);
    console.log(`   ID: ${newState.klados_id}`);
    console.log(`   Collection: ${newState.collection_id}`);
    console.log(`   Endpoint: ${newState.endpoint}`);
    if (newState.api_key_prefix) {
      console.log(`   API Key: ${newState.api_key_prefix}...`);
    }
    console.log(`${'='.repeat(60)}\n`);
  } catch (error) {
    console.error('\n❌ Registration failed:');
    console.error(`   ${error instanceof Error ? error.message : error}`);
    process.exit(1);
  }
}

main();
