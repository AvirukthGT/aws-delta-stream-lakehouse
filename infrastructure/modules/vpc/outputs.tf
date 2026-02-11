output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnets" {
  # Change 'public' to 'public_1'
  value = [aws_subnet.public_1.id, aws_subnet.public_2.id]
}