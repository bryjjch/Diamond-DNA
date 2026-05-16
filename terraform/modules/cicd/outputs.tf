output "github_actions_role_arn" {
  description = "Role ARN to set as the AWS_DEPLOY_ROLE_ARN repository variable in GitHub Actions"
  value       = aws_iam_role.github_actions.arn
}
