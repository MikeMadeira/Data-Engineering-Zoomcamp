variable "credentials" {
  description = "GCP Credentials"
  default     = "./keys/trans-campus/trans-campus-410115-505735bd9928.json"
}


variable "project" {
  description = "Project"
  default     = "trans-campus-410115"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "trans-campus-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}