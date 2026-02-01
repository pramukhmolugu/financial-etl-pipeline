# AWS Lambda Function

# Lambda execution role
resource "aws_iam_role" "lambda_role" {
  name = "financial-etl-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Lambda policy for S3 and CloudWatch
resource "aws_iam_role_policy" "lambda_policy" {
  name = "financial-etl-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.raw_data.arn}/*",
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Lambda function
resource "aws_lambda_function" "transaction_processor" {
  filename      = "../lambda/transaction_processor.zip"
  function_name = "financial-etl-transaction-processor"
  role          = aws_iam_role.lambda_role.arn
  handler       = "transaction_processor.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  memory_size   = 512

  environment {
    variables = {
      ENVIRONMENT = var.environment
    }
  }

  tags = {
    Name    = "Transaction Processor"
    Project = "financial-etl-pipeline"
  }
}

# Permission for S3 to invoke Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transaction_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_data.arn
}
