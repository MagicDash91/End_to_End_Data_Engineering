variable "name" {
  type = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket"
  type        = bool
  default     = true
}

variable "release_retention_days" {
  description = "releases/sha/<sha>.tar.gz objects expire after this many days (releases/current.tar.gz is kept)"
  type        = number
  default     = 30
}
