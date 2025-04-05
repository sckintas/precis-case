variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The region for GCP resources"
  type        = string
}

variable "zone" {
  description = "The availability zone for GCP resources"
  type        = string
}

variable "cluster_name" {
  description = "Name of the Kubernetes cluster"
  type        = string
}

variable "machine_type" {
  description = "The machine type for GKE nodes"
  type        = string
}

variable "staging_dataset_name" {
  description = "BigQuery dataset for raw data"
  type        = string
}

variable "production_dataset_name" {
  description = "BigQuery dataset for transformed data"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the GCS bucket for Data Lake"
  type        = string
}

variable "gcp_storage_class" {
  description = "Storage class for the GCS bucket"
  type        = string
}

variable "account_id" {
  description = "Service Account ID"
  type        = string
}

variable "description" {
  description = "Description for the Service Account"
  type        = string
}

variable "roles" {
  description = "IAM roles assigned to the service account"
  type        = list(string)
}
