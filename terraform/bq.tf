# ===================================================================
# CREATE BIGQUERY DATASETS
# ===================================================================

# Staging Dataset
resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id = var.staging_dataset_name
  project    = var.project_id
  location   = var.region

  labels = {
    environment = "staging"
    owner       = "data-engineering"
  }
}

# Production Dataset
resource "google_bigquery_dataset" "production_dataset" {
  dataset_id = var.production_dataset_name
  project    = var.project_id
  location   = var.region

  labels = {
    environment = "production"
    owner       = "data-engineering"
  }
}
