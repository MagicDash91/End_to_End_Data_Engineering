variable "name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "app_security_group_id" {
  description = "Security group of the pipeline host; the only thing allowed to reach Postgres"
  type        = string
}

variable "engine_version" {
  type    = string
  default = "16"
}

variable "instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "allocated_storage_gb" {
  type    = number
  default = 20
}

variable "master_username" {
  type    = string
  default = "de_admin"
}

variable "backup_retention_days" {
  type    = number
  default = 1
}

variable "deletion_protection" {
  type    = bool
  default = false
}

variable "final_snapshot" {
  type    = bool
  default = false
}

variable "secret_recovery_window_days" {
  description = "0 deletes the secret immediately on destroy (handy for dev re-creates)"
  type        = number
  default     = 0
}
