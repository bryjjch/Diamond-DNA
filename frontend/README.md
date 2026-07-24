# xWAR Engine Frontend

React + TypeScript SPA for the xWAR Engine projection dashboard. Built with
Vite, Tailwind v4, PrimeReact, TanStack Query, and React Router.

Pages:

- **Projections** (`/`) — projected vs. season-to-date-actual leaderboards for
  the current season, with batter/pitcher and XGBoost/Marcel toggles, plus
  player search.
- **Player Detail** (`/player/:playerId`) — projection vs. actuals, season
  history (with Statcast), GMM archetype fit, and nearest-neighbor comparables.
- **Model Accuracy** (`/accuracy`) — XGBoost vs. Marcel backtest summary and
  per-year detail.

## Local development

The dev server proxies `/api/*` to the API Gateway named by
`VITE_DEV_API_PROXY`, so no local backend is required. The URL is not
hardcoded — put it in `frontend/.env.local` (gitignored) or the shell:

```powershell
cd frontend
npm install                                # first time only
echo "VITE_DEV_API_PROXY=https://<api-id>.execute-api.us-east-1.amazonaws.com" > .env.local
npm run dev                                # http://127.0.0.1:5173
```

Get the URL from `terraform output api_endpoint` once the infra rebuild
(REBUILD.md) is applied. Without `VITE_DEV_API_PROXY` the dev server starts
with no proxy and every `/api/*` request fails.

### Which season the site shows

`/api/meta`'s `default_year` is derived from the Lambda's calendar (current year
− 1), so it lags the live season. `src/lib/season.ts` instead probes the newest
plausible season, steps back a year if it has no artifacts, and falls back to
`default_year` — so the site follows the data without a redeploy. Set
`VITE_SEASON_YEAR` (or the API's `WEBAPP_YEAR`) to pin a specific season.

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
  components/    # AppShell, Sidebar, Topbar, LeaderboardTable, PlayerSearch,
                 # ArchetypeProbs, RoleSelect, ModelSelect, SearchInput, ErrorBanner
  pages/         # Home, PlayerDetail, Accuracy
  lib/           # api client, types, stats formatting, theme, queryClient, utils
  App.tsx        # routes
  main.tsx       # mount + providers (PrimeReact, React Query, Router)
```
