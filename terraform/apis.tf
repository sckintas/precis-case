# ===================================================================
# GCP SERVISLERINI AKTIF ETME
# ===================================================================

locals {
  all_project_services = [
    "compute.googleapis.com",
    "container.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ]
}

resource "google_project_service" "enabled_apis" {
  project = var.project_id
  count   = length(local.all_project_services)
  service = local.all_project_services[count.index]

  disable_on_destroy = false
}
