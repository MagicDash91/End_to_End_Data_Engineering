variable "project" {
  description = "Name prefix for every resource. Must match the environment stacks."
  type        = string
  default     = "ecom-medallion"
}

variable "region" {
  type    = string
  default = "ap-southeast-3"
}

variable "github_repository" {
  description = "owner/repo allowed to assume the CI role, e.g. michael/data-engineering-project"
  type        = string
}

variable "create_github_oidc_provider" {
  description = "false if the account already has the token.actions.githubusercontent.com provider"
  type        = bool
  default     = true
}
