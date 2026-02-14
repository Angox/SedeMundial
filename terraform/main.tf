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

# --- TRUCO: Subir imagen dummy para desbloquear la creación de la Lambda ---
resource "null_resource" "initial_image" {
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<EOF
      aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
      docker pull public.ecr.aws/lambda/python:3.11
      docker tag public.ecr.aws/lambda/python:3.11 ${aws_ecr_repository.lambda_repo.repository_url}:latest
      docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOF
  }
  
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
    Statement = [{ Action = "lambda:InvokeFunction", Effect = "Allow", Resource = "*" }] 
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

  schedule_expression = "cron(0 10 1 * ? *)"

  target {
    arn      = aws_lambda_function.ingestor.arn
    role_arn = aws_iam_role.scheduler_role.arn
  }
}

resource "aws_cloudwatch_event_rule" "s3_to_glue_rule" {
  name        = "trigger-glue-on-s3-upload"
  description = "Dispara el Glue job cuando se suben objetos a raw/"

  event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {
        "name": [aws_s3_bucket.data_lake.id]
      },
      "object": {
        "key": [{ "prefix": "raw/" }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_target" {
  rule      = aws_cloudwatch_event_rule.s3_to_glue_rule.name
  target_id = "SendToGlue"
  arn       = aws_glue_job.cleaner.arn
  role_arn  = aws_iam_role.eventbridge_glue_role.arn
}

# ==========================================
# 5. Redshift Serverless
# ==========================================

resource "aws_redshiftserverless_namespace" "stadiums" {
  namespace_name = "stadiums-namespace"
  admin_username = "adminuser"
  admin_user_password = "Password123Temporary!" 
  db_name = "stadiumsdb"
}

resource "aws_redshiftserverless_workgroup" "stadiums_wg" {
  namespace_name = "stadiums-namespace"
  workgroup_name = "stadiums-workgroup"
  base_capacity  = 32
  publicly_accessible = true 
  
  depends_on = [aws_redshiftserverless_namespace.stadiums]

  timeouts {
    create = "60m"
    update = "60m"
    delete = "60m"
  }
}
