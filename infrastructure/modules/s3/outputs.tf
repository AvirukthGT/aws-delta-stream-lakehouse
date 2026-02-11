output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "clean_bucket_name" {
  value = aws_s3_bucket.clean.bucket
}

output "curated_bucket_name" {
  value = aws_s3_bucket.curated.bucket
}