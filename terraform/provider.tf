terraform {
  required_version = ">= 1.0"

  backend "gcs" {
    bucket = "mycrypto-secure-terraform-state-bucket_new" # no space
    prefix = "terraform/state"
  }



  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
