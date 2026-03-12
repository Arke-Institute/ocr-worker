/**
 * Mistral OCR Client
 *
 * Calls Mistral's OCR API to extract text and embedded images from JPEGs.
 * Uses SDK built-in retry with exponential backoff for rate limit handling.
 */

import { Mistral } from '@mistralai/mistralai';

/**
 * Embedded image extracted by Mistral OCR
 */
export interface MistralOCRImage {
  /** Unique ID like "img-0" */
  id: string;
  /** Top-left X coordinate (pixels) */
  topLeftX: number | null;
  /** Top-left Y coordinate (pixels) */
  topLeftY: number | null;
  /** Bottom-right X coordinate (pixels) */
  bottomRightX: number | null;
  /** Bottom-right Y coordinate (pixels) */
  bottomRightY: number | null;
  /** Full data URI: "data:image/jpeg;base64,..." */
  imageBase64?: string | null;
}

/**
 * Result from Mistral OCR
 */
export interface MistralOCRResult {
  /** OCR text in markdown format */
  markdown: string;
  /** Embedded images with coordinates and base64 data */
  images: MistralOCRImage[];
}

/**
 * Call Mistral OCR API with SDK-level retry/backoff
 *
 * The Mistral SDK handles 429/5xx retries internally with exponential backoff,
 * jitter, and Retry-After header support. Configured for bulk workloads:
 * - 2s initial delay, 2min max delay, 1.5x exponent
 * - 5 min total retry window per call
 *
 * @param imageBase64 - Base64 encoded image (without data URI prefix)
 * @param mimeType - MIME type (e.g., "image/jpeg")
 * @param apiKey - Mistral API key
 * @returns OCR result with markdown text and extracted images
 */
export async function callMistralOCR(
  imageBase64: string,
  mimeType: string,
  apiKey: string,
): Promise<MistralOCRResult> {
  const client = new Mistral({
    apiKey,
    retryConfig: {
      strategy: 'backoff',
      backoff: {
        initialInterval: 2000,    // 2s base delay
        maxInterval: 120000,      // 2 min max delay
        exponent: 1.5,
        maxElapsedTime: 300000,   // 5 min total retry window
      },
      retryConnectionErrors: true,
    },
  });

  const response = await client.ocr.process({
    model: 'mistral-ocr-latest',
    document: {
      type: 'image_url',
      imageUrl: `data:${mimeType};base64,${imageBase64}`,
    },
    includeImageBase64: true,
  });

  // Extract first page (we only process single JPEGs)
  const page = response.pages?.[0] as
    | {
        markdown?: string;
        images?: Array<{
          id: string;
          topLeftX?: number;
          topLeftY?: number;
          bottomRightX?: number;
          bottomRightY?: number;
          imageBase64?: string;
        }>;
      }
    | undefined;

  return {
    markdown: page?.markdown || '',
    images: (page?.images || []).map((img) => ({
      id: img.id,
      topLeftX: img.topLeftX ?? null,
      topLeftY: img.topLeftY ?? null,
      bottomRightX: img.bottomRightX ?? null,
      bottomRightY: img.bottomRightY ?? null,
      imageBase64: img.imageBase64 ?? null,
    })),
  };
}
