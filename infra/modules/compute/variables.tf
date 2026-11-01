variable "name" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "instance_type" {
  description = "Airbyte (abctl) wants >= 4 vCPU / 8 GB on its own; Airflow + Spark share the rest"
  type        = string
  default     = "t3.xlarge"
}

variable "root_volume_gb" {
  type    = number
  default = 60
}

variable "artifacts_bucket" {
  type = string
}

variable "artifacts_bucket_arn" {
  type = string
}

variable "db_secret_arn" {
  type = string
}

variable "ingestion_mode" {
  description = "airbyte (install Airbyte on the host) or python (built-in loader, smaller host is enough)"
  type        = string
  default     = "airbyte"
  validation {
    condition     = contains(["airbyte", "python"], var.ingestion_mode)
    error_message = "ingestion_mode must be airbyte or python."
  }
}
