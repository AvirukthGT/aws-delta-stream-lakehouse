resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Bronze Layer (Raw Data)
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-raw-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "Bronze Layer"
  }
}

# Silver Layer (Cleaned Data)
resource "aws_s3_bucket" "clean" {
  bucket        = "${var.project_name}-clean-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "Silver Layer"
  }
}

# Gold Layer (Curated Data)
resource "aws_s3_bucket" "curated" {
  bucket        = "${var.project_name}-curated-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "Gold Layer"
  }
}