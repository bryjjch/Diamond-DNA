# Diamond-DNA

Player archetype clustering for the MLB using Gaussian Mixture Models, served
as a cloud-native web app.

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

Three Lambdas run daily on EventBridge:

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
  common/               # shared S3 / settings / runtime helpers; CLI + Lambda handler wrappers
  data_pipeline/        # bronze → silver → gold ETL
    bronze/             # daily Statcast / running / defence ingest
    silver/             # bronze → silver feature build
    gold/               # silver → gold preprocessing
  ml/                   # archetype clustering, KNN similarity
docker/                 # Dockerfiles + requirements per Lambda image
frontend/               # React + TS SPA (Vercel)
terraform/              # IaC: S3, pipeline Lambdas, API, CI/CD
tests/                  # pytest
```
