terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9.0"
    }
  }
  
  backend "s3" {
    bucket         = "mi-empresa-tf-state-lock" 
    key            = "proyectos/stadiums/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    # AGREGAR ESTO PARA TRABAJO EN EQUIPO
    dynamodb_table = "terraform-locks" 
  }
}

provider "aws" {
  region = "us-east-1"
}
