# xWAR Engine

MLB player performance projections, served as a cloud-native web app.
Formerly **Diamond-DNA**, a player archetype clustering project — the
archetype (GMM) and similarity (KNN) models live on as the comparables
engine behind the projections.

## Revamping

This project is transitioning from archetype clustering (Diamond-DNA) to
player performance projections (xWAR Engine). Many existing features carry
over. Projections will start with next-season projections; rest-of-season
projections could come later as a V2. Target stats:

Batters: PA, AVG/OBP/SLG, wOBA, HR, SB, K%, BB%
Pitchers: IP, ERA, FIP, K%, BB%, WHIP

The repo, docs, and frontend are rebranded to xWAR Engine. AWS resources
(S3 bucket, Lambdas, ECR repos) still carry legacy `diamond-dna` names —
AWS does not allow renaming, so those will be replaced when the
architecture is rebuilt for projections.

## Architecture

```
                ┌──────────────┐
   Vercel ──►   React SPA      │   (frontend/)
                └─────┬────────┘
                      │ fetch  VITE_API_BASE_URL
                      ▼
                ┌──────────────┐      ┌──────────────────┐
   API Gateway  │ HTTP API v2  │ ──►  │ Lambda           │   (src/api/)
                └──────────────┘      │ diamond-dna-api  │
                                      └────────┬─────────┘
                                               │ s3:GetObject (gold/*)
                                               ▼
                                      ┌──────────────────┐
                                      │  S3 Data Lake    │
                                      │  bronze / silver │
                                      │  gold / models   │
                                      └────────▲─────────┘
                                               │
                ┌──────────────────────────────┴───────────────────────┐
                │ Pipeline Lambdas (EventBridge, daily)                │
                │   bronze → silver → gold                             │
                │   src/data_pipeline/{bronze,silver,gold}, docker/    │
                └──────────────────────────────────────────────────────┘
```

| Layer        | Lives in                                    | Deploys via              |
| ------------ | ------------------------------------------- | ------------------------ |
| Frontend     | `frontend/` (Vite + React + TS, Tailwind)   | Vercel (auto on push)    |
| HTTP API     | `src/api/` + `docker/api/`                  | GitHub Actions → ECR → Lambda |
| Pipeline ETL | `src/data_pipeline/{bronze,silver,gold}/`   | Terraform + manual build |
| ML training  | `src/ml/`                                   | Manual / batch CLI       |
| Infra        | `terraform/`                                | `terraform apply`        |

## Data pipeline

Three Lambdas run daily on EventBridge (legacy `diamond-dna-*` resource
names, see [Revamping](#revamping)):

| Time UTC | Lambda                   | Reads             | Writes              |
| -------- | ------------------------ | ----------------- | ------------------- |
| 06:00    | `diamond-dna-statcast-ingestion` | pybaseball  | `bronze/statcast/`  |
| 06:15    | `diamond-dna-silver-feature-build` | bronze | `silver/{role}/`    |
| 06:30    | `diamond-dna-gold-preprocessing`   | silver | `gold/statcast/{role}/` |

ML stages (archetype clustering, KNN similarity) are run manually via the CLIs
under `src/ml/`; their outputs are what the HTTP API serves.

## Layout

```
src/
  api/                  # HTTP API Lambda handler
  common/               # shared S3 / settings / runtime helpers
  data_pipeline/        # bronze → silver → gold ETL
    bronze/             # daily Statcast / running / defence / player-bio ingest
    silver/
      archetype_features/  # bronze → silver archetype feature build
      standard_stats/      # bronze → silver standard stat-line tables
    gold/               # silver → gold preprocessing
  ml/
    archetypes/         # archetype clustering, labeling, finetune sweeps
    knn_neighbours/     # KNN similar-players similarity
docker/                 # Dockerfiles + requirements per Lambda image
frontend/               # React + TS SPA (Vercel)
terraform/              # IaC: S3, pipeline Lambdas, API, CI/CD
tests/                  # pytest
```
