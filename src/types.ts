/**
 * Type definitions for the OCR worker
 */

/**
 * Cloudflare Worker environment bindings
 */
export interface Env {
  /** Klados agent ID (registered in Arke) */
  AGENT_ID: string;

  /** Agent version for logging */
  AGENT_VERSION: string;

  /** Arke agent API key (secret) */
  ARKE_AGENT_KEY: string;

  /** Mistral API key for OCR (secret) */
  MISTRAL_API_KEY: string;

  /** Verification token for endpoint verification (set during registration) */
  VERIFICATION_TOKEN?: string;

  /** Agent ID for verification (used before AGENT_ID is configured) */
  ARKE_VERIFY_AGENT_ID?: string;

  /** Durable Object binding for job processing */
  KLADOS_JOB: DurableObjectNamespace;
}

/**
 * Standard input following file-input-conventions.md
 */
export interface OCRInput {
  /** Specific file key to process. If not provided, auto-detects JPEG. */
  target_file_key?: string;
}

/**
 * File metadata from properties.content map
 */
export interface FileMetadata {
  cid: string;
  content_type: string;
  size: number;
  uploaded_at?: string;
}

/**
 * Properties added after OCR processing
 */
export interface OCROutputProperties {
  /** OCR extracted text in markdown format */
  text: string;
  /** Source of text extraction */
  text_source: 'ocr';
  /** When text was extracted */
  text_extracted_at: string;
  /** Model used for OCR */
  ocr_model: string;
  /** Number of images extracted */
  ocr_images_extracted: number;
  /** Which file key was OCR'd */
  ocr_source_file_key: string;
}

/**
 * Properties for extracted image entities
 */
export interface ExtractedImageProperties {
  /** Label for the extracted image */
  label: string;
  /** Source of extraction */
  extraction_source: 'ocr';
  /** Which file this was extracted from (provenance) */
  source_file_key: string;
  /** Bounding box coordinates from Mistral */
  source_bbox: {
    x1: number | null;
    y1: number | null;
    x2: number | null;
    y2: number | null;
  };
  /** Mistral's image ID (e.g., "img-0") */
  source_image_ref: string;
  /** When the image was extracted */
  extracted_at: string;
  /** Allow additional properties for Record<string, unknown> compatibility */
  [key: string]: unknown;
}

/**
 * Entity with content map in properties
 */
export interface EntityWithContent {
  id: string;
  type: string;
  properties: {
    content?: Record<string, FileMetadata>;
    [key: string]: unknown;
  };
}
