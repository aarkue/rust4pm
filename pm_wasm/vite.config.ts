import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
// https://github.com/vitejs/vite/blob/ec7ee22cf15bed05a6c55693ecbac27cfd615118/packages/vite/src/node/plugins/workerImportMetaUrl.ts#L127-L128
const workerImportMetaUrlRE =
  /\bnew\s+(?:Worker|SharedWorker)\s*\(\s*(new\s+URL\s*\(\s*('[^']+'|"[^"]+"|`[^`]+`)\s*,\s*import\.meta\.url\s*\))/g;

// https://vitejs.dev/config/
export default defineConfig({
  worker: {
    format: "es",
    plugins: () => [
      {
        name: "worker-fix",
        enforce: "pre",
        transform(code, id) {
          if (
            code.includes("new Worker") &&
            code.includes("new URL") &&
            code.includes("import.meta.url")
          ) {
            const result = code.replace(
              workerImportMetaUrlRE,
              `((() => { throw new Error('Nested workers are disabled') })()`,
            );
            return result;
          }
        },
      },
    ],
    rollupOptions: {
      output: {
        chunkFileNames: "assets/worker/[name]-[hash].js",
        assetFileNames: "assets/worker/[name]-[hash].wasm",
      },
    },
  },
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
  plugins: [react()],
});
