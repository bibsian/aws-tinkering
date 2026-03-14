terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  profile = "pluralsight"
  region  = var.aws_region
}

# Example: S3 bucket
resource "aws_s3_bucket" "example" {
  bucket = "${var.project_name}-${random_id.suffix.hex}"

  tags = {
    Project     = var.project_name
    Environment = "sandbox"
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}
