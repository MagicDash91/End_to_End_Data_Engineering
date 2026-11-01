variable "project" {
  type    = string
  default = "ecom-medallion"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "ap-southeast-3"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "instance_type" {
  type    = string
  default = "t3.xlarge"
}

variable "db_instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "db_deletion_protection" {
  type    = bool
  default = false
}

variable "ingestion_mode" {
  type    = string
  default = "airbyte"
}
