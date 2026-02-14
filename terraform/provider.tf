terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Añadimos el proveedor de tiempo aquí
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9.0"
    }
  }
  
  # Backend para guardar el estado
  backend "s3" {
    bucket         = "mi-empresa-tf-state-lock" # <--- ASEGÚRATE DE QUE ESTE ES TU BUCKET REAL
    key            = "proyectos/stadiums/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}
