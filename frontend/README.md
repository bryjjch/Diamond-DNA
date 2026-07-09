# xWAR Engine Frontend

React + TypeScript SPA for the xWAR Engine cluster browser and KNN similarity
viewer. Built with Vite, Tailwind v4, TanStack Query, and React Router.

## Local development

The dev server proxies `/api/*` to the deployed API Gateway, so no local
backend is required:

```powershell
cd frontend
npm install        # first time only
npm run dev        # http://127.0.0.1:5173
```

The proxy target is configured in [vite.config.ts](vite.config.ts) and can be
overridden via `VITE_DEV_API_PROXY`.

## Production (Vercel)

1. Import this repo into Vercel; set the **Root Directory** to `frontend`.
2. Set the environment variable `VITE_API_BASE_URL` to the API Gateway endpoint
   exposed by Terraform (`terraform output api_endpoint`), e.g.
   `https://abc123.execute-api.us-east-1.amazonaws.com`.
3. Vercel auto-detects Vite — no build configuration needed.
4. After the first deploy, lock the API CORS to the Vercel domain by setting
   `api_cors_allow_origins = ["https://your-app.vercel.app"]` in Terraform and
   re-applying.

## Scripts

| Command           | Description                                |
| ----------------- | ------------------------------------------ |
| `npm run dev`     | Vite dev server with API proxy             |
| `npm run build`   | Typecheck + production bundle to `dist/`   |
| `npm run preview` | Serve the production build locally         |
| `npm run lint`    | ESLint                                     |

## Layout

```
src/
  components/    # AppShell, Sidebar, Topbar, SearchInput, RoleSelect, ErrorBanner
  pages/         # ClusterBrowser, SimilarPlayers
  lib/           # api client, types, queryClient, utils
  App.tsx        # routes
  main.tsx       # mount + providers (PrimeReact, React Query, Router)
```
