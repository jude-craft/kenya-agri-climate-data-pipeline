terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# 1. Automate Data Lake (GCS Bucket) Creation
resource "google_storage_bucket" "agri_climate_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
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

# 2. Automate Data Warehouse (BigQuery Dataset) Creation
resource "google_bigquery_dataset" "agri_climate_dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
}