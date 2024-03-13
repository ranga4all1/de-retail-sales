variable "credentials" {
  description = "my credentials"
  default     = "CREDENTIALS"
  # Do NOT upload credentials files to github to repository or anywhere on internet
  # This can be done by adding those files/folders to .gitignore
}

variable "project" {
  description = "Project id"
  #Update the below to your project
  default = "PROJECT_ID"
}

variable "location" {
  description = "Project lopcation"
  #Update the below to your desired location
  default = "LOCATION"
}

variable "region" {
  description = "Project region"
  #Update the below to your desired region
  default = "REGION"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  #Update the below to what you want your dataset to be called
  default = "BQ_DATASET_NAME"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  #Update the below to a unique bucket name
  default = "GCS_BUCKET_NAME"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  #Update the below to your storage class
  default = "GCS_STORAGE_CLASS"
}