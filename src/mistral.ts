/**
 * Mistral OCR Client
 *
 * Calls Mistral's OCR API to extract text and embedded images from JPEGs.
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
 * Calculate exponential backoff delay
 */
function calculateBackoff(attempt: number, baseMs = 1000, maxMs = 30000): number {
  const delay = Math.min(baseMs * Math.pow(2, attempt), maxMs);
  // Add jitter (0-25% of delay)
  return delay + Math.random() * delay * 0.25;
}

/**
 * Sleep for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Call Mistral OCR API with retry logic
 *
 * @param imageBase64 - Base64 encoded image (without data URI prefix)
 * @param mimeType - MIME type (e.g., "image/jpeg")
 * @param apiKey - Mistral API key
 * @param maxRetries - Maximum retry attempts (default: 3)
 * @returns OCR result with markdown text and extracted images
 */
export async function callMistralOCR(
  imageBase64: string,
  mimeType: string,
  apiKey: string,
  maxRetries = 3
): Promise<MistralOCRResult> {
  const client = new Mistral({ apiKey });

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await client.ocr.process({
        model: 'mistral-ocr-latest',
        document: {
          type: 'image_url',
          imageUrl: `data:${mimeType};base64,${imageBase64}`,
        },
        includeImageBase64: true, // Extract embedded images
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
    } catch (error) {
      // Check if we should retry
      const isRetryable =
        error instanceof Error &&
        (error.message.includes('rate limit') ||
          error.message.includes('timeout') ||
          error.message.includes('503') ||
          error.message.includes('502') ||
          error.message.includes('500'));

      if (attempt < maxRetries && isRetryable) {
        const delayMs = calculateBackoff(attempt);
        console.log(
          `Mistral OCR attempt ${attempt + 1} failed, retrying in ${Math.round(delayMs)}ms...`
        );
        await sleep(delayMs);
        continue;
      }

      // Non-retryable or exhausted retries
      throw error;
    }
  }

  // Should not reach here, but TypeScript needs it
  throw new Error('Mistral OCR failed after all retries');
}
