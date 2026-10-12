import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";

// Relative base so the same build works at the GitHub Pages project path
// (/exp-fg-pct/) and from `vite preview`.
export default defineConfig({
  base: "./",
  plugins: [svelte()],
  // Plotly's cartesian bundle (~1.4 MB, ~480 KB gzipped) is its own chunk,
  // loaded only by the tabs that draw Plotly charts.
  build: { chunkSizeWarningLimit: 1600 },
  test: {
    include: ["tests/**/*.test.ts"],
  },
});
