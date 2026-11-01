output "instance_id" {
  value = aws_instance.host.id
}

output "security_group_id" {
  value = aws_security_group.app.id
}
