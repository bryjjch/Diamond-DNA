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
                │   src/bronze, src/silver, src/gold, docker/{layer}   │
                └──────────────────────────────────────────────────────┘
```

| Layer        | Lives in                                    | Deploys via              |
| ------------ | ------------------------------------------- | ------------------------ |
| Frontend     | `frontend/` (Vite + React + TS, Tailwind)   | Vercel (auto on push)    |
| HTTP API     | `src/api/` + `docker/api/`                  | GitHub Actions → ECR → Lambda |
| Pipeline ETL | `src/bronze/`, `src/silver/`, `src/gold/`   | Terraform + manual build |
| ML training  | `src/ml/`                                   | Manual / batch CLI       |
| Infra        | `terraform/`                                | `terraform apply`        |

## Local development

### Run the full stack on your machine

```powershell
# 1. Install Python deps (includes the Flask shim used by the React dev server)
pip install -e ".[dev]"

# 2. Point the API at a local parquet bundle, or AWS S3
$env:WEBAPP_DATA_DIR = "<path-with-archetypes_*.parquet,neighbors_*.parquet>"
# OR
# $env:S3_BUCKET = "<your-bucket>" ; aws sso login   # etc.

# 3. Start the API shim (wraps src/api/handler.py)
diamond-dna-api          # http://127.0.0.1:5001

# 4. In another terminal, start the SPA
cd frontend
npm install              # first time only
npm run dev              # http://127.0.0.1:5173, /api/* proxied to :5001
```

### Test the Lambda handler directly

```powershell
python -c "from src.api.handler import handler; print(handler({'rawPath': '/api/health', 'requestContext': {'http': {'method': 'GET', 'path': '/api/health'}}}, None))"
```

## Deploying to production

### One-time setup

1. **Apply base infrastructure** — provisions S3 bucket, pipeline Lambdas, API
   Lambda, API Gateway, and ECR repos:

   ```powershell
   cd terraform
   terraform init
   terraform apply         # set data_lake_bucket_name in terraform.tfvars
   ```

2. **Build & push the API image** (first deploy — afterwards CI/CD takes over):

   ```powershell
   $EcrUrl  = terraform output -raw api_ecr_repository_url
   $Region  = "us-east-1"   # or your region
   docker build --platform linux/amd64 --provenance=false -f docker/api/Dockerfile -t "${EcrUrl}:latest" .
   aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $EcrUrl.Split('/')[0]
   docker push "${EcrUrl}:latest"
   ```

3. **Enable CI/CD** — provisions the GitHub Actions IAM role:

   ```hcl
   # terraform.tfvars
   enable_cicd  = true
   github_owner = "your-gh-user"
   github_repo  = "Diamond-DNA"
   ```

   ```powershell
   terraform apply
   $RoleArn  = terraform output -raw github_actions_role_arn
   $ApiUrl   = terraform output -raw api_endpoint
   ```

4. **Configure GitHub repo variables** (Settings → Secrets and variables →
   Actions → Variables):

   | Variable                | Value                                  |
   | ----------------------- | -------------------------------------- |
   | `AWS_REGION`            | e.g. `us-east-1`                       |
   | `AWS_ACCOUNT_ID`        | 12-digit account ID                    |
   | `AWS_DEPLOY_ROLE_ARN`   | `$RoleArn` from above                  |
   | `ECR_REPOSITORY`        | `diamond-dna-api` (optional override)  |
   | `LAMBDA_FUNCTION_NAME`  | `diamond-dna-api` (optional override)  |

5. **Connect Vercel:**
   - Import this repo into Vercel.
   - Set **Root Directory** to `frontend`.
   - Add env var `VITE_API_BASE_URL` = `$ApiUrl` from step 3.
   - Vercel auto-detects Vite and deploys.

6. **Lock down CORS** (after Vercel issues the production URL):

   ```hcl
   api_cors_allow_origins = ["https://your-app.vercel.app"]
   ```

   ```powershell
   terraform apply
   ```

### Ongoing deploys

| What changed                       | How it ships                                  |
| ---------------------------------- | --------------------------------------------- |
| `frontend/**`                      | Push to `main` → Vercel auto-deploy            |
| `src/api/**` or `docker/api/**`    | Push to `main` → GitHub Actions → ECR → Lambda |
| Pipeline code (`src/bronze`, etc.) | Manual `docker build && push` + `terraform apply` |
| Infra (`terraform/**`)             | `terraform apply`                              |

## Data pipeline (unchanged)

Three Lambdas run daily on EventBridge:

| Time UTC | Lambda                   | Reads             | Writes              |
| -------- | ------------------------ | ----------------- | ------------------- |
| 06:00    | `diamond-dna-statcast-ingestion` | pybaseball  | `bronze/statcast/`  |
| 06:15    | `diamond-dna-silver-feature-build` | bronze + defence | `silver/{role}/`    |
| 06:30    | `diamond-dna-gold-preprocessing`   | silver           | `gold/statcast/{role}/` |

ML stages (archetype clustering, KNN similarity) are run manually via the CLIs
under `src/ml/`; their outputs are what the HTTP API serves.

## Layout

```
src/
  api/         # HTTP API Lambda handler + Flask local-dev shim
  bronze/      # daily Statcast ingest
  silver/      # bronze → silver feature build
  gold/        # silver → gold preprocessing
  ml/          # archetype clustering, KNN similarity
  pipeline/    # shared S3 / settings helpers
docker/        # Dockerfiles + requirements per Lambda image
frontend/      # React + TS SPA (Vercel)
terraform/     # IaC: S3, pipeline Lambdas, API, CI/CD
tests/         # pytest
```
