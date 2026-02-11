module "networking" {
  source = "./modules/vpc"

  project_name = var.project_name
  aws_region   = var.aws_region
}

module "storage" {
  source = "./modules/s3"

  project_name = var.project_name
}

module "database" {
  source = "./modules/rds"

  project_name = var.project_name
  db_password  = var.db_password
  
  vpc_id     = module.networking.vpc_id
  subnet_ids = module.networking.public_subnets
}