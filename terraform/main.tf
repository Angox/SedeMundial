variable "kaggle_username" {}
variable "kaggle_key" {}

# 1. Bucket S3 para Datos (Raw y Clean)
resource "aws_s3_bucket" "data_lake" {
  bucket_prefix = "stadiums-datalake-"
}

# 2. Repositorio ECR para la imagen de Lambda
resource "aws_ecr_repository" "lambda_repo" {
  name = "stadiums-ingestor"
}

# (Nota: La construcción y push de Docker se hace en GitHub Actions, 
# aquí asumimos que la imagen existirá con tag 'latest')

# 3. Rol IAM para Lambda
resource "aws_iam_role" "lambda_role" {
  name = "stadiums_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

# Permisos básicos para Lambda (Logs y S3)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "lambda_s3_policy" {
  name = "lambda_s3_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = ["s3:PutObject", "s3:ListBucket"], Effect = "Allow", Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"] }]
  })
}
resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

# 4. Lambda Function (Desde Imagen Docker)
resource "aws_lambda_function" "ingestor" {
  function_name = "stadiums-kaggle-ingestor"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  # URL dummy inicial, GitHub Actions actualizará esto
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 300 # 5 minutos para descargar
  memory_size   = 1024 

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.data_lake.bucket
      KAGGLE_USERNAME = var.kaggle_username
      KAGGLE_KEY      = var.kaggle_key
    }
  }
}

# 5. EventBridge Scheduler (Trigger Mensual)
resource "aws_scheduler_schedule" "monthly_trigger" {
  name = "stadiums-monthly-trigger"
  
  flexible_time_window {
    mode = "OFF"
  }

  # Ejecutar el día 1 de cada mes a las 10:00 AM
  schedule_expression = "cron(0 10 1 * ? *)"

  target {
    arn      = aws_lambda_function.ingestor.arn
    role_arn = aws_iam_role.scheduler_role.arn # (Debes crear este rol también, omitido por brevedad)
  }
}

# 6. Redshift Serverless (Opción moderna y escalable)
resource "aws_redshiftserverless_namespace" "stadiums" {
  namespace_name = "stadiums-namespace"
  admin_username = "admin"
  admin_user_password = "Password123!" # Usa Secrets Manager en prod
}

resource "aws_redshiftserverless_workgroup" "stadiums" {
  namespace_name = "stadiums-namespace"
  workgroup_name = "stadiums-workgroup"
  base_capacity  = 32 # Minimo capacidad
  
  depends_on = [aws_redshiftserverless_namespace.stadiums]
}

# 7. Glue Job (Transformación)
resource "aws_glue_job" "cleaner" {
  name     = "stadiums-cleaner"
  role_arn = aws_iam_role.glue_role.arn # Requiere crear rol con permisos S3 y Redshift
  
  command {
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/etl_script.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--TempDir" = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"
  }
}

# Trigger de Glue: Se lanza cuando llega data a RAW
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.glue_trigger_lambda.arn # Lambda intermediaria suele ser necesaria o EventBridge Rule
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
  # Nota: S3 -> EventBridge -> Glue es el patrón moderno preferido sobre S3 Notification directo a Glue
}
