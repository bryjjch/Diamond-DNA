# GitHub Actions OIDC provider (one per AWS account). Skip when an existing
# provider already exists — set var.create_oidc_provider = false and import
# the ARN externally if needed.
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub's published OIDC thumbprint (rotated June 2023). AWS no longer
  # verifies this thumbprint as of mid-2023, but the field is still required.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = var.tags
}

locals {
  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
  repo_sub          = "repo:${var.github_owner}/${var.github_repo}:ref:${var.github_ref_pattern}"
}

data "aws_caller_identity" "current" {}

# Trust policy: only the configured GitHub repo + ref can assume this role.
resource "aws_iam_role" "github_actions" {
  name = "${var.name_prefix}-github-actions"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = local.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
            "token.actions.githubusercontent.com:sub" = local.repo_sub
          }
        }
      }
    ]
  })

  tags = var.tags
}

# Permissions: ECR auth + push to the API repo, and update-function-code on
# the API Lambda. Scoped tight — no IAM, no broad ECR, no other Lambdas.
resource "aws_iam_role_policy" "github_actions_deploy" {
  name = "${var.name_prefix}-github-actions-deploy"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EcrAuthToken"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Sid    = "EcrPushApiImage"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:DescribeImages",
          "ecr:DescribeRepositories",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:ListImages",
          "ecr:PutImage",
          "ecr:UploadLayerPart",
        ]
        Resource = var.api_ecr_repository_arn
      },
      {
        Sid    = "UpdateApiLambda"
        Effect = "Allow"
        Action = [
          "lambda:UpdateFunctionCode",
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:PublishVersion",
        ]
        Resource = var.api_lambda_function_arn
      },
    ]
  })
}
