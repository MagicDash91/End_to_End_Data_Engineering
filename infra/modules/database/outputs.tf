output "address" {
  value = aws_db_instance.this.address
}

output "port" {
  value = aws_db_instance.this.port
}

output "secret_arn" {
  value = aws_secretsmanager_secret.db.arn
}

output "security_group_id" {
  value = aws_security_group.db.id
}
