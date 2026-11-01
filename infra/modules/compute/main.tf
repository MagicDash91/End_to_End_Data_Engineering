# The pipeline host: one EC2 instance running Airflow + Spark (docker compose) and Airbyte (abctl/kind).
# No inbound rules at all -- admin access and the UIs go through SSM Session Manager port forwarding.
resource "aws_security_group" "app" {
  name        = "${var.name}-app"
  description = "Pipeline host: no inbound, all outbound"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.app.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Image pulls, package installs, S3, Secrets Manager, RDS"
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host" {
  name               = "${var.name}-host"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "host" {
  statement {
    sid       = "ReadReleases"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.artifacts_bucket_arn, "${var.artifacts_bucket_arn}/*"]
  }
  statement {
    sid       = "ReadDbSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.db_secret_arn]
  }
}

resource "aws_iam_role_policy" "host" {
  name   = "${var.name}-host"
  role   = aws_iam_role.host.id
  policy = data.aws_iam_policy_document.host.json
}

resource "aws_iam_instance_profile" "host" {
  name = "${var.name}-host"
  role = aws_iam_role.host.name
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_instance" "host" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.host.name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    project          = var.name
    region           = var.region
    artifacts_bucket = var.artifacts_bucket
    db_secret_arn    = var.db_secret_arn
    ingestion_mode   = var.ingestion_mode
  })

  root_block_device {
    volume_size = var.root_volume_gb # Airbyte's images alone are ~10 GB
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2 # containers on the host can reach IMDS for the instance role
  }

  tags = { Name = "${var.name}-host" }

  lifecycle {
    # A newer AL2023 AMI should not replace a running host; rebuild it deliberately with -replace.
    ignore_changes = [ami, user_data]
  }
}
