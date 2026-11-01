output "instance_id" {
  value = module.compute.instance_id
}

output "artifacts_bucket" {
  value = module.storage.bucket
}

output "rds_address" {
  value = module.database.address
}

output "db_secret_arn" {
  value = module.database.secret_arn
}

output "airflow_ui_tunnel" {
  description = "Run locally, then open http://localhost:8080"
  value       = "aws ssm start-session --region ${var.region} --target ${module.compute.instance_id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080"
}

output "airbyte_ui_tunnel" {
  description = "Run locally, then open http://localhost:8000"
  value       = "aws ssm start-session --region ${var.region} --target ${module.compute.instance_id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8000,localPortNumber=8000"
}
