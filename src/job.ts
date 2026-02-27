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
    label: `extracted_${sourceRef}.jpeg`,
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
// Main Processing Function
// =============================================================================

/**
 * Process OCR job
 *
 * @param ctx - Processing context
 * @returns Result with output entity IDs
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger, env, authToken } = ctx;
  const input = (request.input || {}) as OCRInput;

  logger.info('Starting OCR processing', {
    target: request.target_entity,
    targetFileKey: input.target_file_key,
  });

  // =========================================================================
  // Step 1: Fetch target entity
  // =========================================================================

  if (!request.target_entity) {
    throw new Error('No target_entity in request');
  }

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

  // =========================================================================
  // Step 2: Resolve JPEG file using file input conventions
  // =========================================================================

  const { fileKey, fileMeta } = resolveJpegFile(
    target as EntityWithContent,
    input.target_file_key
  );

  // =========================================================================
  // Step 3: Validate file size
  // =========================================================================

  if (fileMeta.size && fileMeta.size > MAX_FILE_SIZE) {
    throw new Error(
      `File '${fileKey}' is too large (${Math.round(fileMeta.size / 1024 / 1024)}MB). ` +
        `Maximum size is ${MAX_FILE_SIZE / 1024 / 1024}MB.`
    );
  }

  logger.info('Processing JPEG', { fileKey, size: fileMeta.size });

  // =========================================================================
  // Step 4: Download JPEG content
  // =========================================================================

  const { data: contentData, error: contentError } = await client.api.GET(
    '/entities/{id}/content',
    {
      params: {
        path: { id: request.target_entity },
        query: { key: fileKey },
      },
      parseAs: 'arrayBuffer',
    }
  );

  if (contentError || !contentData) {
    throw new Error(`Failed to download content: ${JSON.stringify(contentError)}`);
  }

  const imageBase64 = arrayBufferToBase64(contentData as ArrayBuffer);

  // =========================================================================
  // Step 5: Call Mistral OCR
  // =========================================================================

  logger.info('Calling Mistral OCR');
  const ocrResult = await callMistralOCR(
    imageBase64,
    'image/jpeg',
    env.MISTRAL_API_KEY
  );
  logger.info('OCR complete', {
    textLength: ocrResult.markdown.length,
    imagesFound: ocrResult.images.length,
  });

  // =========================================================================
  // Step 6: Create child entities for extracted images (if any)
  // =========================================================================

  const extractedEntities: string[] = [];
  const imageIdMap = new Map<string, string>(); // mistral-id -> arke-entity-id

  for (const img of ocrResult.images) {
    if (!img.imageBase64) continue;

    // Decode base64 data URI
    const imageData = decodeBase64DataUri(img.imageBase64);

    // Create child entity with provenance
    const childEntity = await createExtractedImage(client, {
      collection: request.target_collection,
      parentId: request.target_entity,
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
    logger.info('Created extracted image', {
      entityId: childEntity.id,
      sourceRef: img.id,
    });
  }

  // =========================================================================
  // Step 7: Transform markdown to use arke: URIs for extracted images
  // =========================================================================

  const transformedMarkdown = replaceImageRefs(ocrResult.markdown, imageIdMap);

  // =========================================================================
  // Step 8: Update source entity with OCR text (only if text exists)
  // =========================================================================

  if (transformedMarkdown.trim().length > 0) {
    // Get current tip for CAS-safe update
    const { data: tipData, error: tipError } = await client.api.GET(
      '/entities/{id}/tip',
      {
        params: { path: { id: request.target_entity } },
      }
    );

    if (tipError || !tipData) {
      throw new Error(`Failed to get entity tip: ${JSON.stringify(tipError)}`);
    }

    // Update entity with OCR results
    const { error: updateError } = await client.api.PUT('/entities/{id}', {
      params: { path: { id: request.target_entity } },
      body: {
        expect_tip: tipData.cid,
        properties: {
          text: transformedMarkdown,
          text_source: 'ocr',
          text_extracted_at: new Date().toISOString(),
          ocr_model: 'mistral-ocr-latest',
          ocr_images_extracted: extractedEntities.length,
          ocr_source_file_key: fileKey,
        },
        // Add relationships to extracted images
        relationships_add: extractedEntities.map((id) => ({
          predicate: 'has_extracted',
          peer: id,
          peer_type: 'file',
        })),
      },
    });

    if (updateError) {
      throw new Error(
        `Failed to update entity with OCR text: ${JSON.stringify(updateError)}`
      );
    }

    logger.success('Updated entity with OCR text', {
      textLength: transformedMarkdown.length,
      imagesExtracted: extractedEntities.length,
    });
  } else {
    logger.info('No text extracted from image');
  }

  // =========================================================================
  // Step 9: Return output (source entity updated)
  // =========================================================================

  return {
    outputs: [request.target_entity],
  };
}
