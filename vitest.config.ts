import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // E2E tests run against real API
    testTimeout: 120000,
    hookTimeout: 60000,

    // Single thread to avoid race conditions
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
  },
});
