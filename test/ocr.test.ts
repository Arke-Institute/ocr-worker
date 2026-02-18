/**
 * OCR Worker E2E Tests
 *
 * Tests the OCR worker against a real Arke API instance.
 * Requires:
 * - ARKE_USER_KEY environment variable
 * - KLADOS_ID environment variable (from registration)
 * - Worker deployed to Cloudflare
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';
import {
  configureTestClient,
  createCollection,
  createEntity,
  invokeKlados,
  waitForKladosLog,
  getEntity,
  deleteEntity,
  assertLogCompleted,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const KLADOS_ID = process.env.KLADOS_ID;
const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';

// =============================================================================
// Helper: Upload content to entity
// =============================================================================

async function uploadContent(
  entityId: string,
  key: string,
  content: Buffer,
  contentType: string
): Promise<void> {
  const url = `${ARKE_API_BASE}/entities/${entityId}/content?key=${encodeURIComponent(key)}`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': contentType,
      'Authorization': `ApiKey ${ARKE_USER_KEY}`,
    },
    body: content,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to upload content: ${response.status} ${errorText}`);
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('OCR Worker', () => {
  let testCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string;

  // Skip if not configured
  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!KLADOS_ID) {
      console.warn('Skipping tests: KLADOS_ID not set');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: ARKE_NETWORK,
    });
  });

  // Create test fixtures
  beforeAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Creating test collection...');
    testCollection = await createCollection({
      label: `ocr-worker-test-${Date.now()}`,
      description: 'Test collection for OCR worker',
      roles: { public: ['*:view', '*:invoke'] },
    });
    log(`Created collection: ${testCollection.id}`);
  });

  // Cleanup
  afterAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Cleaning up...');
    try {
      if (testEntity?.id) {
        await deleteEntity(testEntity.id);
        log(`Deleted test entity: ${testEntity.id}`);
      }
      if (testCollection?.id) {
        await deleteEntity(testCollection.id);
        log(`Deleted test collection: ${testCollection.id}`);
      }
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  it('should process a JPEG and extract text', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // 1. Create test entity
    log('Creating test entity...');
    testEntity = await createEntity({
      type: 'document',
      properties: {
        label: 'Faculty Minutes 1850',
        description: 'Test document for OCR',
      },
      collection: testCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);

    // 2. Upload JPEG content
    log('Uploading JPEG content...');
    const jpegPath = join(__dirname, 'fixtures', 'a092_001_001.jpg');
    const jpegContent = readFileSync(jpegPath);
    await uploadContent(testEntity.id, 'scan.jpeg', jpegContent, 'image/jpeg');
    log(`Uploaded ${jpegContent.length} bytes`);

    // 3. Verify content was uploaded
    const entityWithContent = await getEntity(testEntity.id);
    expect(entityWithContent.properties.content).toBeDefined();
    expect(entityWithContent.properties.content['scan.jpeg']).toBeDefined();
    log('Content uploaded successfully');

    // 4. Invoke OCR worker
    log('Invoking OCR worker...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: testEntity.id,
      targetCollection: testCollection.id,
      confirm: true,
    });

    log(`Invocation result: ${JSON.stringify(result)}`);
    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // 5. Wait for completion
    log('Waiting for OCR completion...');
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
    });

    assertLogCompleted(kladosLog);
    log(`OCR completed with status: ${kladosLog.properties.status}`);

    // 6. Verify text was extracted
    log('Verifying OCR results...');
    const updatedEntity = await getEntity(testEntity.id);

    expect(updatedEntity.properties.text).toBeDefined();
    expect(updatedEntity.properties.text_source).toBe('ocr');
    expect(updatedEntity.properties.ocr_model).toBe('mistral-ocr-latest');
    expect(updatedEntity.properties.ocr_source_file_key).toBe('scan.jpeg');

    log('OCR text extracted:');
    log(`  Length: ${(updatedEntity.properties.text as string).length} chars`);
    log(`  Preview: ${(updatedEntity.properties.text as string).slice(0, 200)}...`);

    // Check for expected content (this is the Minute Book cover page)
    const text = updatedEntity.properties.text as string;
    expect(text.toLowerCase()).toContain('minute');
    expect(text.toLowerCase()).toContain('faculty');
  });

  it('should reject entities without JPEG content', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Create entity without any content
    log('Creating entity without JPEG...');
    const entityWithoutJpeg = await createEntity({
      type: 'document',
      properties: {
        label: 'No JPEG Document',
      },
      collection: testCollection.id,
    });
    log(`Created entity: ${entityWithoutJpeg.id}`);

    // Invoke OCR worker - should fail
    log('Invoking OCR worker (expecting failure)...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: entityWithoutJpeg.id,
      targetCollection: testCollection.id,
      confirm: true,
    });

    log(`Invocation result: ${JSON.stringify(result)}`);
    expect(result.status).toBe('started');

    // Wait for completion
    log('Waiting for OCR to fail...');
    const kladosLog = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 3000,
    });

    // Should be in error state
    expect(kladosLog.properties.status).toBe('error');
    log(`OCR failed as expected: ${kladosLog.properties.status}`);

    // Cleanup
    await deleteEntity(entityWithoutJpeg.id);
  });
});
