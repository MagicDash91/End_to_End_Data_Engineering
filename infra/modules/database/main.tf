# RDS PostgreSQL holding both the shop (source) and warehouse (bronze/silver/gold) databases.
# The databases and schemas themselves are created by sql/init/init.sh from the EC2 host.
resource "random_password" "master" {
  length = 32
  # RDS rejects '/', '@', '"' and spaces.
  override_special = "!#%^*-_=+"
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "db" {
  name        = "${var.name}-db"
  description = "Postgres, reachable only from the pipeline host"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "from_app" {
  security_group_id            = aws_security_group.db.id
  referenced_security_group_id = var.app_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "Postgres from the pipeline host (Airflow, Spark, dbt, Airbyte)"
}

resource "aws_db_instance" "this" {
  identifier     = "${var.name}-pg"
  engine         = "postgres"
  engine_version = var.engine_version # major only: RDS picks the region's default minor version
  instance_class = var.instance_class

  allocated_storage     = var.allocated_storage_gb
  max_allocated_storage = var.allocated_storage_gb * 2
  storage_type          = "gp3"
  storage_encrypted     = true

  username = var.master_username
  password = random_password.master.result
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false
  multi_az               = false

  backup_retention_period    = var.backup_retention_days
  auto_minor_version_upgrade = true
  apply_immediately          = true
  deletion_protection        = var.deletion_protection
  skip_final_snapshot        = !var.final_snapshot
  final_snapshot_identifier  = var.final_snapshot ? "${var.name}-pg-final" : null
}

resource "aws_secretsmanager_secret" "db" {
  name                    = "${var.name}/rds-master"
  description             = "Master credentials for ${aws_db_instance.this.identifier}, read by the EC2 host at deploy time"
  recovery_window_in_days = var.secret_recovery_window_days
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = var.master_username
    password = random_password.master.result
    host     = aws_db_instance.this.address
    port     = aws_db_instance.this.port
  })
}
