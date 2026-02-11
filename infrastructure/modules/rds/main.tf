# 1. Subnet Group (Groups the 2 subnets for RDS use)
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = var.subnet_ids # Passed from Root -> VPC Module

  tags = {
    Name = "My DB Subnet Group"
  }
}

# 2. Security Group (Firewall)
resource "aws_security_group" "rds_sg" {
  name        = "${var.project_name}-rds-sg"
  description = "Allow Postgres inbound traffic"
  vpc_id      = var.vpc_id # Passed from Root -> VPC Module

  # Ingress: Allow port 5432 (Postgres) from ANYWHERE (0.0.0.0/0)
  # WARNING: For a portfolio project, this is fine. 
  # In real life, restrict this to your IP address.
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Egress: Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# 3. The Database Instance
resource "aws_db_instance" "postgres" {
  identifier           = "${var.project_name}-db"
  allocated_storage    = 20
  storage_type         = "gp2"
  engine               = "postgres"
  engine_version       = "17.7"
  instance_class       = "db.t3.micro"
  db_name              = "ecommerce_db"
  username             = "dbadmin"
  password             = var.db_password
  parameter_group_name = "default.postgres17"
  skip_final_snapshot  = true
  publicly_accessible  = true

  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  tags = {
    Name = "Primary Database"
  }
}