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
  
  # Backend para guardar el estado (MODIFICA EL BUCKET SI ES NECESARIO)
  backend "s3" {
    bucket         = "mi-empresa-tf-state-lock" 
    key            = "proyectos/stadiums/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}
