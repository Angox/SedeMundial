variable "kaggle_username" {}
variable "kaggle_key" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ==========================================
# 1. Almacenamiento (S3 y ECR)
# ==========================================

# Bucket S3 para Datos (Raw, Temp, Scripts)
resource "aws_s3_bucket" "data_lake" {
  bucket_prefix = "stadiums-datalake-"
  force_destroy = true # ¡Cuidado en producción! Esto borra el bucket aunque tenga datos al hacer destroy
}

# Activar notificaciones de EventBridge para este bucket (Crucial para el trigger de Glue)
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_lake.id
  eventbridge = true
}

# Repositorio ECR para la imagen de Lambda
resource "aws_ecr_repository" "lambda_repo" {
  name = "stadiums-ingestor"
  force_delete = true
}


# ==========================================
# 2. Roles de IAM (Seguridad)
# ==========================================

# --- Rol para Lambda (Ingesta) ---
resource "aws_iam_role" "lambda_role" {
  name = "stadiums_lambda_ingest_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Política de acceso a S3 (La reutilizaremos para Glue también)
resource "aws_iam_policy" "s3_access_policy" {
  name = "stadiums_s3_access_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { 
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"], 
        Effect = "Allow", 
        Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"] 
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# --- Rol para Glue (Transformación) ---
# (Este era uno de los errores que te faltaba)
resource "aws_iam_role" "glue_role" {
  name = "stadiums_glue_etl_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "glue.amazonaws.com" } }]
  })
}

# Permisos básicos para que Glue funcione
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Permiso para leer/escribir en nuestro bucket S3 (Reutilizamos la política)
resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}


# --- Rol para EventBridge Scheduler (Trigger Mensual) ---
# (Este era otro error que te faltaba)
resource "aws_iam_role" "scheduler_role" {
  name = "stadiums_scheduler_invoke_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" } }]
  })
}

resource "aws_iam_policy" "scheduler_invoke_policy" {
  name = "scheduler_invoke_lambda_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "lambda:InvokeFunction", Effect = "Allow", Resource = aws_lambda_function.ingestor.arn }]
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_attach" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_invoke_policy.arn
}

# --- Rol para EventBridge Rule (Trigger de Glue al llegar datos a S3) ---
resource "aws_iam_role" "eventbridge_glue_role" {
  name = "stadiums_
