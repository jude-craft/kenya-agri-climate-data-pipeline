variable "credentials" {
  description = "Path to the Google Cloud Service Account JSON key"
  default     = "./credentials/gcp-service-account.json"
}

variable "project" {
  description = "Your Google Cloud Project ID"
  type        = string
  default     = "kestra-sandbox-504410"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location for BigQuery and GCS"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "kenya_agri_market"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "kenya-agri-climate-lake-bucket"
}
