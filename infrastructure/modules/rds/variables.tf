variable "project_name" {
  description = "Passed from root"
  type        = string
}

variable "db_password" {
  description = "Passed from root"
  type        = string
  sensitive   = true
}

variable "vpc_id" {
  description = "The VPC ID where the DB will live"
  type        = string
}

variable "subnet_ids" {
  description = "The subnets where the DB will live"
  type        = list(string)
}