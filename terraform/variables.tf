variable "credentials" {
  description = "My Credentials"
  default     = "credentials/gc-creds.json"
}

variable "project" {
  description = "Project"
  default     = "white-defender-410709"
}

variable "region" {
  description = "Region"
  default     = "europe-west1-b"
}

variable "location" {
  description = "Project Location"
  default     = "europe-west1-b"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "bq_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "white-defender-410709-taxi-data-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}