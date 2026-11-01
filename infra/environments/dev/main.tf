terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80, < 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Partial config: `terraform init -backend-config=backend.hcl` (see backend.hcl.example).
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.project, Environment = var.environment, ManagedBy = "terraform" }
  }
}

locals {
  name = "${var.project}-${var.environment}"
}

module "network" {
  source     = "../../modules/network"
  name       = local.name
  cidr_block = var.vpc_cidr
}

module "storage" {
  source = "../../modules/storage"
  name   = local.name
}

module "compute" {
  source               = "../../modules/compute"
  name                 = local.name
  region               = var.region
  vpc_id               = module.network.vpc_id
  subnet_id            = module.network.public_subnet_ids[0]
  instance_type        = var.instance_type
  artifacts_bucket     = module.storage.bucket
  artifacts_bucket_arn = module.storage.bucket_arn
  db_secret_arn        = module.database.secret_arn
  ingestion_mode       = var.ingestion_mode
}

module "database" {
  source                = "../../modules/database"
  name                  = local.name
  vpc_id                = module.network.vpc_id
  subnet_ids            = module.network.private_subnet_ids
  app_security_group_id = module.compute.security_group_id
  instance_class        = var.db_instance_class
  deletion_protection   = var.db_deletion_protection
}
