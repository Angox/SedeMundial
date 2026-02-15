variable "kaggle_username" {}
variable "kaggle_key" {}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
# Obtenemos las zonas disponibles (us-east-1a, us-east-1b, etc.)
data "aws_availability_zones" "available" {
  state = "available"
}

# ==========================================
# 1. Red y VPC (CORREGIDO PARA MULTI-AZ)
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

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

# --- CAMBIO IMPORTANTE: Crear 3 Subnets en 3 Zonas distintas ---
resource "aws_subnet" "public" {
  count                   = 3 # Creamos 3 subredes
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24" # 10.0.1.0, 10.0.2.0, 10.0.3.0
  map_public_ip_on_launch = true
  # Asigna cada subnet a una zona diferente (a, b, c)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  
  tags = { Name = "stadiums-public-subnet-${count.index}" }
}

# Asociar las 3 subredes a la tabla de rutas
resource "aws_route_table_association" "public_assoc" {
  count          = 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public_rt.id
}

# ==========================================
# 2. IAM - Equipo y Consola
# ==========================================

resource "aws_iam_group" "developers" {
  name = "stadiums-dev-team"
}

resource "aws_iam_group_policy_attachment" "dev_access" {
  group      = aws_iam_group.developers.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

resource "aws_iam_user" "member_1" {
  name = "equipo-persona1"
  force_destroy = true
}

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
# 3. Storage & Docker (CORREGIDO RUTA)
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
      # 1. Login
      aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
      
      # 2. Intentar borrar la imagen antigua para evitar conflictos de manifiesto (si falla no importa)
      aws ecr batch-delete-image --repository-name stadiums-ingestor --image-ids imageTag=latest || true
      
      # 3. Construir usando el modo LEGACY (DOCKER_BUILDKIT=0)
      # Esto garantiza formato Docker V2 Schema 2 compatible con Lambda
      export DOCKER_BUILDKIT=0
      docker build --platform linux/amd64 -t ${aws_ecr_repository.lambda_repo.repository_url}:latest ../src/lambda
      
      # 4. Subir
      docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOF
  }
  
  # Cambiamos el trigger para forzar que se ejecute siempre
  triggers = {
    always_run = timestamp()
  }
  
  depends_on = [aws_ecr_repository.lambda_repo]
}

# ==========================================
# 4. Lambdas
# ==========================================

resource "aws_iam_role" "lambda_role" {
  name = "stadiums_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_full" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_lambda_function" "ingestor" {
  function_name = "stadiums-ingestor"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 600
  memory_size   = 2048
  
  # AGREGAR ESTA LÍNEA
  architectures = ["x86_64"]
  
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

resource "aws_lambda_function" "cleaner" {
  function_name = "stadiums-cleaner-etl"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 900
  memory_size   = 1024
  
  # AGREGAR ESTA LÍNEA
  architectures = ["x86_64"]
  
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

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cleaner.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake.arn
}

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
  
  # CAMBIO IMPORTANTE: Pasamos TODOS los IDs de las 3 subnets creadas
  subnet_ids             = aws_subnet.public[*].id
  security_group_ids     = [aws_security_group.redshift_sg.id]
  publicly_accessible    = true
}

# ==========================================
# 7. AWS Location Service (Geocoding)
# ==========================================

resource "aws_location_place_index" "main" {
  index_name  = "stadiums-place-index"
  data_source = "Esri" # O "Here", ambos son excelentes proveedores
  description = "Indice para geolocalizar estadios"
}

# Actualizar permisos de la Lambda Cleaner
resource "aws_iam_policy" "location_policy" {
  name        = "stadiums-location-policy"
  description = "Permite a la lambda buscar lugares"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "geo:SearchPlaceIndexForText"
        Resource = aws_location_place_index.main.index_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_location_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.location_policy.arn
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


# ==========================================
# TRIGGER MENSUAL (EventBridge)
# ==========================================

# 1. La Regla: Define CUÁNDO se ejecuta
resource "aws_cloudwatch_event_rule" "monthly_trigger" {
  name        = "stadiums-monthly-ingest"
  description = "Dispara la ingesta de estadios el dia 1 de cada mes"
  
  # Sintaxis Cron: Minuto Hora DiaMes Mes DiaSemana Año
  # Esto se ejecuta a las 00:00 UTC del día 1 de cada mes
  schedule_expression = "cron(0 0 1 * ? *)"
}

# 2. El Objetivo: Define QUÉ se ejecuta (Tu Lambda)
resource "aws_cloudwatch_event_target" "trigger_ingestor_lambda" {
  rule      = aws_cloudwatch_event_rule.monthly_trigger.name
  target_id = "TriggerIngestorLambda"
  arn       = aws_lambda_function.ingestor.arn
}

# 3. El Permiso: Autoriza a EventBridge a invocar tu Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.monthly_trigger.arn
}
