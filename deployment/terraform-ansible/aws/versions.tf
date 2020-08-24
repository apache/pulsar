terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "3.3.0"
    }
    random = {
      source = "hashicorp/random"
    }
  }
  required_version = ">= 0.13"
}
