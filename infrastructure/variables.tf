variable "aws_region" {
  description = "The AWS region to deploy to"
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "The name prefix for all resources"
  type        = string
  default     = "event-driven-lakehouse"
}

variable "db_password" {
  description = "The password for the RDS database"
  type        = string
  sensitive   = true
}