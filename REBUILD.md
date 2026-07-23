# xWAR Engine — Infra Rebuild Runbook

One-time runbook to recreate the AWS + Vercel deployment under the new
`xwar-engine` names. The repo is already fully rebranded; `terraform/backend.hcl`
and `terraform/terraform.tfvars` are already written with the new values.
Delete this file when the rebuild is done.

**State of the world:** the old compute (Lambdas, API GW, ECR) and the old
Terraform state backend are deleted. The old data bucket
`s3://diamond-dna-data-lake` **still exists and holds the only copy of the
data** — it is never referenced by the new Terraform state, so nothing here
can destroy it. Keep it until step 8.

## 0. Prerequisites

```sh
brew install terraform
# any container runtime works; colima is the lightweight option:
brew install colima docker && colima start --arch x86_64   # or use Docker Desktop
```

AWS CLI is already configured (account 173256371253, us-east-1, profile `default`).

## 1. Bootstrap the Terraform state backend

```sh
aws s3api create-bucket --bucket xwar-engine-terraform-state --region us-east-1
aws s3api put-bucket-versioning --bucket xwar-engine-terraform-state \
  --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket xwar-engine-terraform-state \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws dynamodb create-table --table-name xwar-engine-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1
```

## 2. Init + first apply (bucket & ECR repos only)

Image-based Lambdas can't be created until their ECR repos contain an image,
so the first apply is targeted:

```sh
cd terraform
terraform init -backend-config=backend.hcl
terraform apply \
  -target=module.s3 \
  -target=module.api.aws_ecr_repository.api \
  -target=module.lambda.aws_ecr_repository.statcast_ingestion \
  -target=module.lambda.aws_ecr_repository.silver_feature_build \
  -target=module.lambda.aws_ecr_repository.gold_preprocessing
```

Creates `xwar-engine-data-lake` + four empty ECR repos.

## 3. Build & push the Lambda images

```sh
cd ..   # repo root — build context is the repo root
REGISTRY=173256371253.dkr.ecr.us-east-1.amazonaws.com
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$REGISTRY"

for pair in "bronze xwar-engine-statcast-ingestion" \
            "silver xwar-engine-silver-feature-build" \
            "gold   xwar-engine-gold-preprocessing" \
            "api    xwar-engine-api"; do
  set -- $pair
  docker build --platform linux/amd64 --provenance=false \
    -f "docker/$1/Dockerfile" -t "$REGISTRY/$2:latest" .
  docker push "$REGISTRY/$2:latest"
done
```

## 4. Full apply

```sh
cd terraform && terraform apply
terraform output api_endpoint   # save this — used in steps 6 and 7
```

Creates the Lambdas, API Gateway, EventBridge schedules, and log groups.

## 5. Migrate the data lake

```sh
aws s3 sync s3://diamond-dna-data-lake s3://xwar-engine-data-lake
# verify counts/bytes match:
aws s3 ls s3://diamond-dna-data-lake  --recursive --summarize | tail -2
aws s3 ls s3://xwar-engine-data-lake --recursive --summarize | tail -2
```

## 6. CI/CD

The old GitHub Actions workflow and its Terraform (`modules/cicd`, the
`enable_cicd`/`github_owner`/`github_repo` vars, the
`github_actions_role_arn` output) have been removed. Re-add a workflow and
its OIDC role from scratch when you're ready to automate deploys again.

## 7. Vercel

1. Create a new Vercel project named `xwar-engine`, root directory `frontend/`.
2. Set env var `VITE_API_BASE_URL` = the `api_endpoint` output.
3. After the first deploy, lock CORS: in `terraform.tfvars` set
   `api_cors_allow_origins = ["https://<your-project>.vercel.app"]` and `terraform apply`.
4. Update the dev proxy target in `frontend/vite.config.ts` (line ~25) to the
   new `api_endpoint` URL — it currently points at the deleted API Gateway.

## 8. Verify, then decommission the old bucket

- `curl <api_endpoint>/health` → `"service": "xwar-engine-api"`.
- Manually invoke `xwar-engine-statcast-ingestion` and confirm it writes to
  `s3://xwar-engine-data-lake/bronze/...`.
- Frontend loads on the Vercel domain with no CORS errors.

Only after all of the above: `s3://diamond-dna-data-lake` can be emptied and
deleted (irreversible — it's the original copy of the data).
