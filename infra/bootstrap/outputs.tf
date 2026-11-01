output "state_bucket" {
  description = "Put this in infra/environments/dev/backend.hcl"
  value       = aws_s3_bucket.tfstate.bucket
}

output "github_actions_role_arn" {
  description = "Set as the AWS_ROLE_ARN variable in the GitHub repository"
  value       = aws_iam_role.github_actions.arn
}
