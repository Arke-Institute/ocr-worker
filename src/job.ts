/**
 * OCR Job Processing Logic
 *
 * Processes JPEG images using Mistral OCR:
 * 1. Fetches target entity and resolves JPEG file
 * 2. Downloads JPEG content
 * 3. Calls Mistral OCR API
 * 4. Creates child entities for extracted images
 * 5. Updates source entity with OCR text
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger, KladosRequest, Output } from '@arke-institute/rhiza';
import { callMistralOCR } from './mistral';
import type {
  Env,
  OCRInput,
  FileMetadata,
  EntityWithContent,
  ExtractedImageProperties,
} from './types';

/** Maximum file size for OCR processing (20MB) */
const MAX_FILE_SIZE = 20 * 1024 * 1024;

/**
 * Context provided to processJob
 */
export interface ProcessContext {
  /** The original request */
  request: KladosRequest;

  /** Arke client for API calls */
  client: ArkeClient;

  /** Logger for messages (stored in the klados_log) */
  logger: KladosLogger;

  /** SQLite storage for checkpointing long operations */
  sql: SqlStorage;

  /** Worker environment bindings (secrets, vars, DO namespaces) */
  env: Env;

  /** Network-specific auth token (from getKladosConfig) */
  authToken: string;
}

/**
 * Result returned from processJob
 */
export interface ProcessResult {
  /** Output entity IDs (or OutputItems with routing properties) */
  outputs?: Output[];

  /** If true, DO will reschedule alarm and call processJob again */
  reschedule?: boolean;
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * List files from entity's properties.content map
 */
function listEntityFiles(
  target: EntityWithContent
): Array<{ key: string; content_type?: string; size?: number }> {
  const content = target.properties.content;
  if (!content || typeof content !== 'object') return [];

  return Object.entries(content).map(([key, meta]) => ({
    key,
    content_type: meta.content_type,
    size: meta.size,
  }));
}

/**
 * Resolve JPEG file following file-input-conventions.md
 *
 * Priority:
 * 1. Explicit target_file_key if provided
 * 2. Auto-detect JPEG by MIME type
 */
function resolveJpegFile(
  target: EntityWithContent,
  targetFileKey?: string
): { fileKey: string; fileMeta: FileMetadata } {
  const files = listEntityFiles(target);

  // 1. Explicit file key - highest priority
  if (targetFileKey) {
    const file = files.find((f) => f.key === targetFileKey);
    if (!file) {
      throw new Error(`File '${targetFileKey}' not found on entity`);
    }
    if (file.content_type !== 'image/jpeg') {
      throw new Error(
        `File '${targetFileKey}' is not a JPEG (got ${file.content_type})`
      );
    }
    return {
      fileKey: targetFileKey,
      fileMeta: target.properties.content![targetFileKey],
    };
  }

  // 2. Auto-detect JPEG from files
  const jpegFile = files.find((f) => f.content_type === 'image/jpeg');
  if (jpegFile) {
    return {
      fileKey: jpegFile.key,
      fileMeta: target.properties.content![jpegFile.key],
    };
  }

  throw new Error(
    'No JPEG file found on entity. Provide target_file_key or attach a JPEG.'
  );
}

/**
 * Convert ArrayBuffer to base64 string
 */
function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

/**
 * Decode base64 data URI to Uint8Array
 */
function decodeBase64DataUri(dataUri: string): Uint8Array {
  const commaIndex = dataUri.indexOf(',');
  const base64Data = commaIndex >= 0 ? dataUri.slice(commaIndex + 1) : dataUri;
  return Uint8Array.from(atob(base64Data), (c) => c.charCodeAt(0));
}

/**
 * Replace image references in markdown with arke: URIs
 *
 * Transforms: ![img-0.jpeg](img-0.jpeg)
 * To:         ![img-0.jpeg](arke:ENTITY_ID)
 */
function replaceImageRefs(
  markdown: string,
  imageIdMap: Map<string, string>
): string {
  return markdown.replace(
    /!\[([^\]]*)\]\(([^)]+)\)/g,
    (match, alt, src) => {
      // src might be "img-0.jpeg" or just "img-0"
      const baseRef = src.replace(/\.(jpeg|jpg|png)$/i, '');
      const entityId = imageIdMap.get(baseRef) || imageIdMap.get(src);
      return entityId ? `![${alt}](arke:${entityId})` : match;
    }
  );
}

/**
 * Create an extracted image entity with content upload
 */
async function createExtractedImage(
  client: ArkeClient,
  opts: {
    collection: string;
    parentId: string;
    parentType: string;
    sourceFileKey: string;
    sourceRef: string;
    bbox: {
      x1: number | null;
      y1: number | null;
      x2: number | null;
      y2: number | null;
    };
    imageData: Uint8Array;
    apiBase: string;
    authToken: string;
  }
): Promise<{ id: string }> {
  const {
    collection,
    parentId,
    parentType,
    sourceFileKey,
    sourceRef,
    bbox,
    imageData,
    apiBase,
    authToken,
  } = opts;

  const properties: ExtractedImageProperties = {
    label: `Extracted Image ${sourceRef.replace(/^img-/, '')}`,
    extraction_source: 'ocr',
    source_file_key: sourceFileKey,
    source_bbox: bbox,
    source_image_ref: sourceRef,
    extracted_at: new Date().toISOString(),
  };

  // Create file entity with provenance relationship
  const { data: entity, error: createError } = await client.api.POST(
    '/entities',
    {
      body: {
        type: 'file',
        collection,
        properties: properties,
        relationships: [
          { predicate: 'extracted_from', peer: parentId, peer_type: parentType },
        ],
      },
    }
  );

  if (createError || !entity) {
    throw new Error(
      `Failed to create extracted image entity: ${JSON.stringify(createError)}`
    );
  }

  // Upload the image content using fetch (binary upload not well supported by SDK types)
  const uploadUrl = `${apiBase}/entities/${entity.id}/content?key=content&filename=${sourceRef}.jpeg`;
  const uploadResponse = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'image/jpeg',
      'Authorization': `ApiKey ${authToken}`,
    },
    body: imageData,
  });

  if (!uploadResponse.ok) {
    const errorText = await uploadResponse.text();
    throw new Error(
      `Failed to upload extracted image content: ${uploadResponse.status} ${errorText}`
    );
  }

  return { id: entity.id };
}

// =============================================================================
// Single Page OCR
// =============================================================================

/**
 * OCR a single JPEG entity and return its text + extracted images.
 * Does NOT update the entity — caller is responsible for that.
 */
async function ocrSinglePage(
  ctx: ProcessContext,
  entityId: string,
  targetFileKey?: string,
): Promise<{
  markdown: string;
  extractedEntities: string[];
  imageIdMap: Map<string, string>;
}> {
  const { request, client, logger, env, authToken } = ctx;

  // Fetch entity
  const { data: target, error: fetchError } = await client.api.GET(
    '/entities/{id}',
    { params: { path: { id: entityId } } },
  );
  if (fetchError || !target) {
    throw new Error(`Failed to fetch entity: ${entityId}`);
  }

  // Resolve and validate JPEG
  const { fileKey, fileMeta } = resolveJpegFile(
    target as EntityWithContent,
    targetFileKey,
  );
  if (fileMeta.size && fileMeta.size > MAX_FILE_SIZE) {
    throw new Error(
      `File '${fileKey}' too large (${Math.round(fileMeta.size / 1024 / 1024)}MB). Max ${MAX_FILE_SIZE / 1024 / 1024}MB.`,
    );
  }

  // Download and OCR
  const { data: contentData, error: contentError } = await client.api.GET(
    '/entities/{id}/content',
    {
      params: {
        path: { id: entityId },
        query: { key: fileKey },
      },
      parseAs: 'arrayBuffer',
    },
  );
  if (contentError || !contentData) {
    throw new Error(`Failed to download content: ${JSON.stringify(contentError)}`);
  }

  const imageBase64 = arrayBufferToBase64(contentData as ArrayBuffer);
  const ocrResult = await callMistralOCR(imageBase64, 'image/jpeg', env.MISTRAL_API_KEY);

  // Create extracted image entities
  const extractedEntities: string[] = [];
  const imageIdMap = new Map<string, string>();

  for (const img of ocrResult.images) {
    if (!img.imageBase64) continue;
    const imageData = decodeBase64DataUri(img.imageBase64);
    const childEntity = await createExtractedImage(client, {
      collection: request.target_collection,
      parentId: entityId,
      parentType: target.type,
      sourceFileKey: fileKey,
      sourceRef: img.id,
      bbox: {
        x1: img.topLeftX,
        y1: img.topLeftY,
        x2: img.bottomRightX,
        y2: img.bottomRightY,
      },
      imageData,
      apiBase: request.api_base,
      authToken,
    });
    extractedEntities.push(childEntity.id);
    imageIdMap.set(img.id, childEntity.id);
  }

  const transformedMarkdown = replaceImageRefs(ocrResult.markdown, imageIdMap);

  return { markdown: transformedMarkdown, extractedEntities, imageIdMap };
}

/**
 * Update an entity with OCR text results (CAS-safe)
 */
async function updateEntityWithOcrText(
  client: ProcessContext['client'],
  entityId: string,
  markdown: string,
  extractedEntities: string[],
  fileKey: string,
): Promise<void> {
  if (markdown.trim().length === 0) return;

  const { data: tipData, error: tipError } = await client.api.GET(
    '/entities/{id}/tip',
    { params: { path: { id: entityId } } },
  );
  if (tipError || !tipData) {
    throw new Error(`Failed to get entity tip: ${JSON.stringify(tipError)}`);
  }

  const { error: updateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: entityId } },
    body: {
      expect_tip: tipData.cid,
      properties: {
        text: markdown,
        text_source: 'ocr',
        text_extracted_at: new Date().toISOString(),
        ocr_model: 'mistral-ocr-latest',
        ocr_images_extracted: extractedEntities.length,
        ocr_source_file_key: fileKey,
      },
      relationships_add: extractedEntities.map((id) => ({
        predicate: 'has_extracted',
        peer: id,
        peer_type: 'file',
      })),
    },
  });

  if (updateError) {
    throw new Error(`Failed to update entity with OCR text: ${JSON.stringify(updateError)}`);
  }
}

// =============================================================================
// Page Group OCR
// =============================================================================

/**
 * Process a page_group entity: OCR all pages in parallel, concatenate text,
 * update both individual pages and the group entity.
 */
async function processPageGroup(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger } = ctx;
  const groupEntityId = request.target_entity!;

  // Fetch group entity (relationships included by default)
  const { data: groupEntity, error: groupError } = await client.api.GET(
    '/entities/{id}',
    {
      params: { path: { id: groupEntityId } },
    },
  );
  if (groupError || !groupEntity) {
    throw new Error(`Failed to fetch page_group: ${groupEntityId}`);
  }

  // Find contains_page relationships to get individual page entity IDs
  const props = groupEntity.properties as Record<string, unknown>;
  const pageNumbers = (props.page_numbers || []) as number[];
  const relationships = (groupEntity.relationships || []) as Array<{
    predicate: string;
    peer: string;
    properties?: Record<string, unknown>;
  }>;
  const pageRelationships = relationships.filter(r => r.predicate === 'contains_page');

  if (pageRelationships.length === 0) {
    throw new Error(`page_group ${groupEntityId} has no contains_page relationships`);
  }

  const pageEntityIds = pageRelationships.map(r => r.peer);

  logger.info('Processing page group', {
    groupId: groupEntityId,
    pageCount: pageEntityIds.length,
    pageNumbers,
  });

  // OCR all pages in parallel
  const ocrResults = await Promise.all(
    pageEntityIds.map(pageId => ocrSinglePage(ctx, pageId)),
  );

  // Fetch page entities to get page_number for ordering
  const pageEntities = await Promise.all(
    pageEntityIds.map(async (pageId) => {
      const { data, error } = await client.api.GET('/entities/{id}', {
        params: { path: { id: pageId } },
      });
      if (error || !data) throw new Error(`Failed to fetch page ${pageId}`);
      const p = data.properties as Record<string, unknown>;
      return { id: data.id, page_number: (p.page_number as number) || 0 };
    }),
  );

  // Sort by page_number and build results in order
  const indexed = pageEntityIds.map((id, i) => ({
    id,
    page_number: pageEntities[i].page_number,
    ocr: ocrResults[i],
  }));
  indexed.sort((a, b) => a.page_number - b.page_number);

  logger.info('OCR complete for all pages', {
    pages: indexed.map(p => ({
      page: p.page_number,
      textLength: p.ocr.markdown.length,
      images: p.ocr.extractedEntities.length,
    })),
  });

  // Update individual page entities with their own OCR text
  for (const page of indexed) {
    await updateEntityWithOcrText(
      client, page.id, page.ocr.markdown, page.ocr.extractedEntities, 'content',
    );
  }

  // Build concatenated text with page markers (same format as digital mode)
  const concatenatedText = indexed
    .map(p => `--- Page ${p.page_number} ---\n${p.ocr.markdown}`)
    .join('\n\n');

  // Update the group entity with concatenated text
  if (concatenatedText.trim().length > 0) {
    const { data: tipData, error: tipError } = await client.api.GET(
      '/entities/{id}/tip',
      { params: { path: { id: groupEntityId } } },
    );
    if (tipError || !tipData) {
      throw new Error(`Failed to get group tip: ${JSON.stringify(tipError)}`);
    }

    const allExtractedImages = indexed.flatMap(p => p.ocr.extractedEntities);

    const { error: updateError } = await client.api.PUT('/entities/{id}', {
      params: { path: { id: groupEntityId } },
      body: {
        expect_tip: tipData.cid,
        properties: {
          text: concatenatedText,
          text_source: 'ocr',
          text_extracted_at: new Date().toISOString(),
          ocr_model: 'mistral-ocr-latest',
          ocr_pages_processed: indexed.length,
          ocr_images_extracted: allExtractedImages.length,
        },
      },
    });

    if (updateError) {
      throw new Error(`Failed to update group with OCR text: ${JSON.stringify(updateError)}`);
    }

    logger.success('Updated page group with concatenated OCR text', {
      textLength: concatenatedText.length,
      pagesProcessed: indexed.length,
      imagesExtracted: allExtractedImages.length,
    });
  }

  // Build outputs: group entity + all extracted images from all pages
  const outputs: Output[] = [];
  outputs.push({ entity_id: groupEntityId, entity_class: 'text' });

  for (const page of indexed) {
    for (const imageId of page.ocr.extractedEntities) {
      outputs.push({ entity_id: imageId, entity_class: 'extracted_image' });
    }
  }

  logger.info('Returning outputs', {
    total: outputs.length,
    groupEntity: 1,
    extractedImages: outputs.length - 1,
  });

  return { outputs };
}

// =============================================================================
// Main Processing Function
// =============================================================================

/**
 * Process OCR job - handles both single pages and page groups
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger, env, authToken } = ctx;
  const input = (request.input || {}) as OCRInput;

  logger.info('Starting OCR processing', {
    target: request.target_entity,
    targetFileKey: input.target_file_key,
  });

  if (!request.target_entity) {
    throw new Error('No target_entity in request');
  }

  // =========================================================================
  // Check if target is a page_group
  // =========================================================================
  const { data: target, error: fetchError } = await client.api.GET(
    '/entities/{id}',
    {
      params: { path: { id: request.target_entity } },
    }
  );

  if (fetchError || !target) {
    throw new Error(`Failed to fetch target: ${request.target_entity}`);
  }

  logger.info('Fetched target entity', {
    id: target.id,
    type: target.type,
  });

  // Route to page group handler if applicable
  if (target.type === 'page_group') {
    logger.info('Detected page_group entity, processing as group');
    return processPageGroup(ctx);
  }

  // =========================================================================
  // Single page processing (original flow)
  // =========================================================================

  const { fileKey, fileMeta } = resolveJpegFile(
    target as EntityWithContent,
    input.target_file_key
  );

  if (fileMeta.size && fileMeta.size > MAX_FILE_SIZE) {
    throw new Error(
      `File '${fileKey}' is too large (${Math.round(fileMeta.size / 1024 / 1024)}MB). ` +
        `Maximum size is ${MAX_FILE_SIZE / 1024 / 1024}MB.`
    );
  }

  logger.info('Processing JPEG', { fileKey, size: fileMeta.size });

  const { markdown, extractedEntities } = await ocrSinglePage(
    ctx,
    request.target_entity,
    input.target_file_key,
  );

  // Update source entity with OCR text
  await updateEntityWithOcrText(
    client, request.target_entity, markdown, extractedEntities, fileKey,
  );

  if (markdown.trim().length > 0) {
    logger.success('Updated entity with OCR text', {
      textLength: markdown.length,
      imagesExtracted: extractedEntities.length,
    });
  } else {
    logger.info('No text extracted from image');
  }

  // Build outputs with routing properties
  const outputs: Output[] = [];
  outputs.push({ entity_id: request.target_entity, entity_class: 'text' });

  for (const imageId of extractedEntities) {
    outputs.push({ entity_id: imageId, entity_class: 'extracted_image' });
  }

  logger.info('Returning outputs', {
    total: outputs.length,
    textEntities: 1,
    extractedImages: extractedEntities.length,
  });

  return { outputs };
}
