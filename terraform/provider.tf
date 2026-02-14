terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Backend S3 para guardar el estado de Terraform (Best Practice)
  backend "s3" {
    bucket         = "mi-empresa-tf-state-lock" # CRÉALO MANUALMENTE PRIMERO
    key            = "proyectos/stadiums/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}
