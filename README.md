# xWAR Engine

xWAR Engine projects future MLB player performance (next-season batting and pitching lines) served as a live web app. Under the projections sits a comparable engine: a GMM archetype model plus kNN similarity that finds each player's closest historical analogues.

<img width="1508" height="744" alt="Screenshot 2026-07-29 at 11 16 32 AM" src="https://github.com/user-attachments/assets/03650dc7-733a-4e16-864f-460131d6306b" />

<img width="1495" height="751" alt="Screenshot 2026-07-29 at 11 17 18 AM" src="https://github.com/user-attachments/assets/6a08853b-1aad-4036-8701-1ebd5988cee4" />

<img width="1503" height="749" alt="Screenshot 2026-07-29 at 11 17 34 AM" src="https://github.com/user-attachments/assets/63c6f739-dd85-4d8b-bb8b-82e814fc1cf8" />

The data runs on a three-stage lake. A daily EventBridge triggered Step Functions pipeline ingests Statcast, running, defence, and bio data, builds features and standard stat lines, and produces model-ready matrices. Each stage is an independent Lambda so a partial failure doesn't fail the run. Projections are generated from the gold layer and served through an HTTP API on Lambda, with a React frontend on Vercel.

For batters the engine projects PA, AVG/OBP/SLG, wOBA, HR, SB, K%, and BB%.
For pitchers, IP, ERA, FIP, K%, BB%, and WHIP.

## Architecture

```
                ┌──────────────┐
   Vercel ──►   React SPA      │   (frontend/)
                └─────┬────────┘
                      │ fetch  VITE_API_BASE_URL
                      ▼
                ┌──────────────┐      ┌──────────────────┐
   API Gateway  │ HTTP API v2  │ ──►  │ Lambda           │   (src/api/)
                └──────────────┘      │ xwar-engine-api  │
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
                │ Pipeline Step Function (EventBridge, daily 22:00 UTC)│
                │   bronze → silver → gold Lambda chain                │
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

One EventBridge rule (22:00 UTC daily, after the upstream APIs have published the
prior day's data) starts the `xwar-engine-data-pipeline` Step Functions state
machine, which runs the three stage Lambdas as a dependency chain — each stage
waits for the previous one and the chain stops on a full failure (partial results
proceed):

| Order | Lambda | Orchestrates | Reads | Writes |
| ----- | ------ | ------------ | ----- | ------ |
| 1 | `xwar-engine-bronze-ingestion` | statcast, running, defence, bio, standard (`bronze_build`) | pybaseball / MLB APIs | `bronze/*` |
| 2 | `xwar-engine-silver-feature-build` | archetype features + standard stats (`silver_build`) | bronze | `silver/{role}/` |
| 3 | `xwar-engine-gold-preprocessing` | archetype preprocessing + performance matrices (`gold_build`) | silver | `gold/features/{archetypes,performance_prediction}/{role}/` |

ML stages (archetype clustering, KNN similarity, Marcel baseline) are run
manually via the CLIs under `src/ml/`. They read `gold/features/` and split
their outputs by consumer:

| Prefix                                    | Holds                                              |
| ----------------------------------------- | -------------------------------------------------- |
| `gold/features/{dataset}/{role}/year=Y/`  | model-ready feature tables (`archetypes`, `performance_prediction`) |
| `gold/predictions/{artifact}/{role}/year=Y/` | inference outputs meant to be served                |
| `models/{model}/{role}/year=Y/`           | fitted estimators + training/evaluation metadata     |

Keys are built by `src/common/lake_keys.py` — add prefixes there, not inline.

## Layout

```
src/
  api/                  # HTTP API Lambda handler
  common/               # shared S3 / settings / runtime helpers
  data_pipeline/        # bronze → silver → gold ETL
    bronze/             # daily Statcast / running / defence / player-bio ingest
    silver/             # silver_build.py orchestrates both silver steps
      archetype_features/  # bronze → silver archetype feature build
      standard_stats/      # bronze → silver standard stat-line tables
    gold/               # silver → gold preprocessing (gold_build.py orchestrates)
  ml/
    archetypes/         # archetype clustering, labeling, finetune sweeps
    knn_neighbours/     # KNN similar-players similarity
docker/                 # Dockerfiles + requirements per Lambda image
frontend/               # React + TS SPA (Vercel)
terraform/              # IaC: S3, pipeline Lambdas, API, CI/CD
tests/                  # pytest
```
