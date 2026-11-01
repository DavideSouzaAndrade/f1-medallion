import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Em dev, /api vai para a API de BI local (uvicorn na 8080). Em produção, VITE_API_URL
// aponta para o Cloud Run da API de BI.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: { "/api": process.env.VITE_DEV_API ?? "http://localhost:8080" } },
});
