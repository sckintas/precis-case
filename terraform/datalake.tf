# ===================================================================
# DATA LAKE BUCKET
# ===================================================================

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "data_lake_bucket" {
  name          = "${var.data_lake_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false
  storage_class = var.gcp_storage_class

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  versioning {
    enabled = true
  }

  labels = {
    environment = "data-lake"
    owner       = "data-engineering"
  }
}
