variable "kaggle_username" {}
variable "kaggle_key" {}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
# Obtenemos las zonas disponibles para alta disponibilidad (Redshift lo prefiere)
data "aws_availability_zones" "available" {
  state = "available"
}

# ==========================================
# 1. Red y VPC (Multi-AZ)
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

# Creamos 3 Subnets en 3 Zonas distintas para Redshift Serverless
resource "aws_subnet" "public" {
  count                   = 3
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24" # 10.0.1.0, 10.0.2.0, 10.0.3.0
  map_public_ip_on_launch = true
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  
  tags = { Name = "stadiums-public-subnet-${count.index}" }
}

resource "aws_route_table_association" "public_assoc" {
  count          = 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public_rt.id
}

# ==========================================
# 2. IAM - Gestión de Usuarios y Permisos
# ==========================================

# 1. Creamos el grupo para tus compañeros
resource "aws_iam_group" "data_engineers" {
  name = "data-engineers-group"
}

# 2. Les damos permisos de ADMINISTRADOR (Libertad total)
resource "aws_iam_group_policy_attachment" "admin_access" {
  group      = aws_iam_group.data_engineers.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# 3. ¡EL TRUCO! Bloqueamos explícitamente ver el dinero/facturas
resource "aws_iam_group_policy" "deny_billing" {
  name  = "BlockBillingAccess"
  group = aws_iam_group.data_engineers.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Deny",
        Action = [
          "aws-portal:*",       # Consola de facturación antigua
          "billing:*",          # Servicio de facturación
          "ce:*",               # Cost Explorer (Explorador de costos)
          "cur:*",              # Reportes de uso
          "budgets:*",          # Presupuestos
          "payments:*",         # Pagos
          "tax:*"               # Impuestos
        ],
        Resource = "*"
      }
    ]
  })
}

# 4. Creamos los usuarios (Sustituye los nombres 'juan' y 'ana' por los reales)
resource "aws_iam_user" "partner_1" {
  name          = "juan-ingeniero" # CAMBIAR NOMBRE
  force_destroy = true
}

resource "aws_iam_user" "partner_2" {
  name          = "ana-ingeniera"  # CAMBIAR NOMBRE
  force_destroy = true
}

# 5. Los añadimos al grupo
resource "aws_iam_group_membership" "engineers_membership" {
  name = "engineers-membership"
  users = [
    aws_iam_user.partner_1.name,
    aws_iam_user.partner_2.name
  ]
  group = aws_iam_group.data_engineers.name
}

# 6. Generamos contraseñas de consola para ellos
# IMPORTANTE: Esto creará una contraseña inicial que verás en el output
resource "aws_iam_user_login_profile" "partner_1_login" {
  user                    = aws_iam_user.partner_1.name
  password_reset_required = true
  # No usamos PGP para simplificarte la vida, la contraseña saldrá en el output de Terraform
}

resource "aws_iam_user_login_profile" "partner_2_login" {
  user                    = aws_iam_user.partner_2.name
  password_reset_required = true
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
      # 1. Login
      aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
      
      # 2. Limpieza (opcional)
      aws ecr batch-delete-image --repository-name stadiums-ingestor --image-ids imageTag=latest || true
      
      # 3. Build & Push (AMD64 para Lambda estándar)
      export DOCKER_BUILDKIT=0
      docker build --platform linux/amd64 -t ${aws_ecr_repository.lambda_repo.repository_url}:latest ../src/lambda
      docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOF
  }
  
  triggers = {
    always_run = timestamp()
  }
  
  depends_on = [aws_ecr_repository.lambda_repo]
}

# ==========================================
# 4. Configuración de IAM para Lambdas
# ==========================================

resource "aws_iam_role" "lambda_role" {
  name = "stadiums_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

# Permisos Básicos + S3
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_full" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# --- NUEVO: Permiso para usar Redshift Data API ---
resource "aws_iam_policy" "lambda_redshift_data_policy" {
  name        = "lambda-redshift-data-access"
  description = "Permite a Lambda ejecutar SQL en Redshift Serverless via API"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "redshift-data:ExecuteStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:DescribeStatement",
          "redshift-data:ListStatements",
          "redshift-data:BatchExecuteStatement",
          "redshift-serverless:GetCredentials"
        ]
        Resource = "*" # Data API no siempre soporta resource restrictions granulares fácilmente
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_redshift_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_redshift_data_policy.arn
}

# ==========================================
# 5. Redshift Serverless
# ==========================================

# --- NUEVO: Rol para que Redshift pueda leer de S3 (COPY Command) ---
resource "aws_iam_role" "redshift_s3_role" {
  name = "redshift-s3-access-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_attach" {
  role       = aws_iam_role.redshift_s3_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

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
  
  # Vinculamos el Rol S3 al Namespace
  iam_roles = [aws_iam_role.redshift_s3_role.arn]
  
  tags = { Env = "Dev" }
}

resource "aws_redshiftserverless_workgroup" "stadiums" {
  workgroup_name = "stadiums-wg"
  namespace_name = aws_redshiftserverless_namespace.stadiums.namespace_name
  base_capacity  = 8 
  
  subnet_ids         = aws_subnet.public[*].id
  security_group_ids = [aws_security_group.redshift_sg.id]
  publicly_accessible = true
}

# ==========================================
# 6. AWS Location Service (Geocoding)
# ==========================================

resource "aws_location_place_index" "main" {
  index_name  = "stadiums-place-index"
  data_source = "Esri"
  description = "Indice para geolocalizar estadios"
}

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
# 7. Funciones Lambda
# ==========================================

resource "aws_lambda_function" "ingestor" {
  function_name = "stadiums-ingestor"
  role          = aws_iam_role.lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  timeout       = 600
  memory_size   = 2048
  architectures = ["x86_64"]
  
  image_config {
    command = ["main.handler"]
  }

  environment {
    variables = {
      S3_BUCKET_NAME  = aws_s3_bucket.data_lake.bucket
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
  architectures = ["x86_64"]
  
  image_config {
    command = ["main.cleaner_handler"]
  }

  environment {
    variables = {
      S3_BUCKET_NAME    = aws_s3_bucket.data_lake.bucket
      PLACE_INDEX       = aws_location_place_index.main.index_name
      # --- NUEVO: Variables para conexión a Redshift ---
      REDSHIFT_WG_NAME  = aws_redshiftserverless_workgroup.stadiums.workgroup_name
      REDSHIFT_DB       = aws_redshiftserverless_namespace.stadiums.db_name
      REDSHIFT_ROLE_ARN = aws_iam_role.redshift_s3_role.arn
    }
  }
  depends_on = [null_resource.build_docker]
}

# Trigger de S3 para Cleaner
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
    filter_prefix       = "raw/antimoni_football-stadiums/"
    filter_suffix       = ".csv"
  }
  
  depends_on = [aws_lambda_permission.allow_s3]
}

# ==========================================
# 8. Trigger Mensual (EventBridge)
# ==========================================

resource "aws_cloudwatch_event_rule" "monthly_trigger" {
  name                = "stadiums-monthly-ingest"
  description         = "Dispara la ingesta de estadios el dia 1 de cada mes"
  schedule_expression = "cron(0 0 1 * ? *)"
}

resource "aws_cloudwatch_event_target" "trigger_ingestor_lambda" {
  rule      = aws_cloudwatch_event_rule.monthly_trigger.name
  target_id = "TriggerIngestorLambda"
  arn       = aws_lambda_function.ingestor.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.monthly_trigger.arn
}

# ==========================================
# 9. Outputs
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

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.stadiums.workgroup_name
}

output "partner_1_password" {
  value = aws_iam_user_login_profile.partner_1_login.password
  sensitive = false # Para que te la muestre en consola
}

output "partner_2_password" {
  value = aws_iam_user_login_profile.partner_2_login.password
  sensitive = false
}
