# H:\MSc\sentinel-flow\terraform\main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2" # London - Essential for DCTS 
}

# This creates a unique ID so your bucket name doesn't clash with anyone else's
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# The actual S3 Bucket where your Python script will drop data
resource "aws_s3_bucket" "landing_zone" {
  bucket = "sentinel-flow-raw-${random_id.bucket_suffix.hex}"

  tags = {
    Name        = "SentinelFlow Raw Data"
    Environment = "Pilot"
    Owner       = "ShriHari"
  }
}

# Output the name so we can use it in our Python script later
output "s3_bucket_name" {
  value = aws_s3_bucket.landing_zone.bucket
}