output "database_endpoint" {
  description = "The endpoint of the RDS instance"
  # We access the output of the 'database' module
  value       = module.database.db_endpoint
}

output "vpc_id" {
  description = "The ID of the VPC created"
  # We access the output of the 'networking' module
  value       = module.networking.vpc_id
}

output "s3_raw_bucket" {
  value = module.storage.raw_bucket_name
}