terraform {
  required_version = ">= 1.0"

  # Configured via -backend-config=backend.hcl during init
  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}
