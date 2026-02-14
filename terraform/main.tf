variable "kaggle_username" {}
variable "kaggle_key" {}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ==========================================
# 1. Almacenamiento (S3 y ECR)
# ==========================================

resource "aws_s3_bucket" "data_lake" {
  bucket_prefix = "stadiums-datalake-"
  force_destroy = true 
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket      = aws_s3_bucket.data_lake.id
  eventbridge = true
}

resource "aws_ecr_repository" "lambda_repo" {
  name         = "stadiums-ingestor"
  force_delete = true
}

# --- TRUCO MAGICO: Subir imagen dummy para desbloquear la creación de la Lambda ---
# Esto descarga una imagen pequeña de AWS pública y la sube a tu repo
# para que Terraform encuentre "algo" y no falle. Luego GitHub Actions pondrá la real.
resource "null_resource" "initial_image" {
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<EOF
      # Loguearse en ECR
      aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
      
      # Bajar imagen base ligera (Python) de ECR Público
      docker pull public.ecr.aws/lambda/python:3.11
      
      # Etiquetarla para NUESTRO repo privado
      docker tag public.ecr.aws/lambda/python:3.11 ${aws_ecr_repository.lambda_repo.repository_url}:latest
      
      # Subirla
      docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOF
  }
  
  # Solo ejecutar si el repo cambia (o la primera vez)
  triggers = {
    repo_url = aws_ecr_repository.lambda_repo.repository_url
  }
  
  depends_on = [aws_ecr_repository.lambda_repo]
}

# ==========================================
# 2. Roles de IAM
# ==========================================

# --- Lambda Role ---
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

# --- Glue Role ---
resource "aws_iam_role" "glue_role" {
  name = "stadiums_glue_etl_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "glue.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# --- Scheduler Role ---
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
    Statement = [{ Action = "lambda:InvokeFunction", Effect = "Allow", Resource = "*" }] # Usamos * temporalmente para evitar dependencias circulares
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_attach" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_invoke_policy.arn
}

# --- EventBridge Role ---
resource "aws_iam_role" "eventbridge_glue_role" {
  name = "stadiums_eb_trigger_glue_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "events.amazonaws.com" } }]
  })
}

resource "aws_iam_policy" "eb_start_glue_policy" {
  name = "eb_start_glue_job_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "glue:StartJobRun", Effect = "Allow", Resource = "*" }]
  })
}

resource "aws_iam_role_policy_attachment" "eb_glue_attach" {
  role       = aws_iam_role.eventbridge_glue_role.name
  policy_arn = aws_iam_policy.eb_start_glue_policy.arn
}

# ==========================================
# 3. Cómputo (Lambda y Glue)
# ==========================================

resource "aws_lambda_function" "ingestor" {
  function_name = "stadiums-kaggle-ingestor"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 600
  memory_size   = 2048 

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.data_lake.bucket
      KAGGLE_USERNAME = var.kaggle_username
      KAGGLE_KEY      = var.kaggle_key
    }
  }

  # Esperamos a que la imagen dummy se haya subido
  depends_on = [null_resource.initial_image, aws_iam_role_policy_attachment.lambda_basic_execution]
}

resource "aws_glue_job" "cleaner" {
  name     = "stadiums-cleaner"
  role_arn = aws_iam_role.glue_role.arn 
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/etl_script.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--TempDir" = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"
    "--job-language" = "python"
    "--enable-metrics" = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  # Esperamos a que el rol se propague
  depends_on = [aws_iam_role.glue_role, aws_iam_role_policy_attachment.glue_service_role]
}

# ==========================================
# 4. Orquestación
# ==========================================

resource "aws_scheduler_schedule" "monthly_trigger" {
  name = "stadiums-monthly-ingest-trigger"
  
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "
