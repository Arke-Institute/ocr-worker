/**
 * CloudflareKeyStore - KeyStore implementation for Cloudflare Workers
 *
 * Uses wrangler CLI to manage secrets. Note that Cloudflare doesn't
 * support reading secrets via CLI, so get() always returns null.
 */

import { execSync } from 'child_process';
import type { KeyStore } from '@arke-institute/rhiza/registration';

export class CloudflareKeyStore implements KeyStore {
  private cwd: string;

  /**
   * @param cwd - Working directory where wrangler.jsonc is located
   */
  constructor(cwd: string = process.cwd()) {
    this.cwd = cwd;
  }

  async get(_name: string): Promise<string | null> {
    // Cloudflare doesn't support reading secrets via CLI
    return null;
  }

  async set(name: string, value: string): Promise<void> {
    // Use echo with pipe to avoid shell escaping issues
    execSync(`echo "${value}" | wrangler secret put ${name}`, {
      cwd: this.cwd,
      stdio: 'pipe',
    });
  }

  async delete(name: string): Promise<void> {
    try {
      execSync(`wrangler secret delete ${name} --force`, {
        cwd: this.cwd,
        stdio: 'pipe',
      });
    } catch {
      // Ignore if secret doesn't exist
    }
  }

  async exists(_name: string): Promise<boolean> {
    // Cloudflare doesn't support checking if a secret exists
    return false;
  }
}
