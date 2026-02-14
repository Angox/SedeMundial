variable "kaggle_username" {}
variable "kaggle_key" {}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_availability_zones" "available" {}

# ==========================================
# 1. Red y VPC
# ==========================================

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "stadiums-vpc" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = data.aws_availability_zones.available.names[0]
}

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "public_assoc" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public_rt.id
}

# ==========================================
# 2. IAM - Equipo y Consola
# ==========================================

# Grupo de desarrolladores
resource "aws_iam_group" "developers" {
  name = "stadiums-dev-team"
}

# Permiso PowerUser
resource "aws_iam_group_policy_attachment" "dev_access" {
  group      = aws_iam_group.developers.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

# Usuario 1
resource "aws_iam_user" "member_1" {
  name = "equipo-persona1"
  force_destroy = true
}

# Perfil de Login para consola
resource "aws_iam_user_login_profile" "member_1_login" {
  user    = aws_iam_user.member_1.name
  pgp_key = "keybase:terraform" 
  
  lifecycle {
    ignore_changes = [password_reset_required, password_length]
  }
}

resource "aws_iam_group_membership" "team" {
  name = "team-membership"
  users = [aws_iam_user.member_1.name]
  group = aws_iam_group.developers.name
}

# ==========================================
# 3. Storage & Docker
# ==========================================

resource "aws_s3_bucket" "data_lake" {
  bucket_prefix = "stadiums-datalake-"
  force_destroy = true 
}

resource "aws_ecr_repository" "lambda_repo" {
  name         = "stadiums-ingestor"
  force_delete = true
}

resource "null_resource" "build_docker" {
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<EOF
      aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
      docker build -t ${aws_ecr_repository.lambda_repo.repository_url}:latest ./src/lambda
      docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOF
  }
  triggers = { always_run = timestamp() }
  depends_on = [aws_ecr_repository.lambda_repo]
}

# ==========================================
# 4. Lambdas (Ingesta + Limpieza)
# ==========================================

resource "aws_iam_role" "lambda_role" {
  name = "stadiums_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

# Corrección aquí: líneas separadas
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Corrección aquí: líneas separadas
resource "aws_iam_role_policy_attachment" "lambda_s3_full" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Lambda 1: Ingesta
resource "aws_lambda_function" "ingestor" {
  function_name = "stadiums-ingestor"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 600
  memory_size   = 2048
  
  image_config {
    command = ["main.handler"]
  }

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.data_lake.bucket
      KAGGLE_USERNAME = var.kaggle_username
      KAGGLE_KEY      = var.kaggle_key
    }
  }
  depends_on = [null_resource.build_docker]
}

# Lambda 2: Cleaner (Reemplaza a GLUE)
resource "aws_lambda_function" "cleaner" {
  function_name = "stadiums-cleaner-etl"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 300
  memory_size   = 1024
  
  image_config {
    command = ["main.cleaner_handler"]
  }

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.data_lake.bucket
    }
  }
  depends_on = [null_resource.build_docker]
}

# Permiso para S3 invoque a Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cleaner.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake.arn
}

# Trigger S3 -> Lambda Cleaner
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.cleaner.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.allow_s3]
}

# ==========================================
# 5. Redshift Serverless
# ==========================================

resource "aws_security_group" "redshift_sg" {
  name        = "stadiums-redshift-sg"
  vpc_id      = aws_vpc.main.id
  
  # Corrección: Bloques ingress/egress en varias líneas
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshiftserverless_namespace" "stadiums" {
  namespace_name      = "stadiums-ns"
  db_name             = "stadiumsdb"
  admin_username      = "adminuser"
  admin_user_password = "Password123Temp!"
  tags = { Env = "Dev" }
}

resource "aws_redshiftserverless_workgroup" "stadiums" {
  workgroup_name = "stadiums-wg"
  namespace_name = aws_redshiftserverless_namespace.stadiums.namespace_name
  base_capacity  = 8 
  
  subnet_ids             = [aws_subnet.public.id]
  security_group_ids     = [aws_security_group.redshift_sg.id]
  publicly_accessible    = true
}

# ==========================================
# 6. Outputs
# ==========================================

output "bucket_name" { 
  value = aws_s3_bucket.data_lake.id 
}

output "console_login_url" {
  value = "https://${data.aws_caller_identity.current.account_id}.signin.aws.amazon.com/console"
}

output "redshift_host" {
  value = aws_redshiftserverless_workgroup.stadiums.endpoint[0].address
}
