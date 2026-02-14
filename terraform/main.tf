provider "aws" {
  region = "us-east-1"
}

# --- 1. Almacenamiento (S3) ---
resource "aws_s3_bucket" "data_lake" {
  bucket = "mi-proyecto-data-lake-raw"
}

# --- 2. AWS Glue (ETL) ---
resource "aws_glue_job" "clean_data" {
  name     = "kaggle_clean_job"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/glue_etl_job.py"
    python_version  = "3"
  }
  default_arguments = {
    "--TempDir" = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"
  }
}

# --- 3. Evento: S3 -> Lambda -> Glue ---
# (Se omite la definición de la Lambda por brevedad, pero la lógica es:
# Un bucket notification dispara una Lambda que llama a client.start_job_run)

# --- 4. Redshift Serverless ---
resource "aws_redshiftserverless_namespace" "namespace" {
  namespace_name = "mi-redshift-ns"
  iam_roles      = [aws_iam_role.redshift_bedrock_role.arn]
}

resource "aws_redshiftserverless_workgroup" "workgroup" {
  namespace_name = "mi-redshift-ns"
  workgroup_name = "mi-redshift-wg"
  base_capacity  = 32 # RPUs
}

# --- 5. IAM Role para Redshift + Bedrock ---
resource "aws_iam_role" "redshift_bedrock_role" {
  name = "RedshiftBedrockIntegrationRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

# Permiso explícito para invocar Bedrock
resource "aws_iam_policy" "bedrock_access" {
  name = "RedshiftBedrockAccess"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "bedrock:InvokeModel",
        "bedrock:ListFoundationModels"
      ]
      Effect   = "Allow"
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_bedrock" {
  role       = aws_iam_role.redshift_bedrock_role.name
  policy_arn = aws_iam_policy.bedrock_access.arn
}
