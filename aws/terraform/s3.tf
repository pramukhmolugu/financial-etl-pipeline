# AWS S3 Buckets for ETL Pipeline

# Raw data bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = "financial-etl-raw-data-${var.environment}"
  
  tags = {
    Name        = "ETL Raw Data"
    Environment = var.environment
    Project     = "financial-etl-pipeline"
  }
}

# Processed data bucket
resource "aws_s3_bucket" "processed_data" {
  bucket = "financial-etl-processed-data-${var.environment}"
  
  tags = {
    Name        = "ETL Processed Data"
    Environment = var.environment
    Project     = "financial-etl-pipeline"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle policy for old data
resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "delete-old-data"
    status = "Enabled"

    expiration {
      days = 90
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# S3 notification for Lambda trigger
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.raw_data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transaction_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
