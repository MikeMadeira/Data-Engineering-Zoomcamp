terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = "./keys/trans-campus/trans-campus-410115-505735bd9928.json"
  project = "trans-campus-410115"
  region = "europe-west2"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "trans-campus-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}