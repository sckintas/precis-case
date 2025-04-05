resource "google_container_cluster" "gke_cluster" {
  name     = var.cluster_name
  location = var.zone # Replaced hardcoded "us-east1-b" ✅
  project  = var.project_id

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  lifecycle {
    ignore_changes = [
      remove_default_node_pool,
      initial_node_count
    ]
  }
}

resource "google_container_node_pool" "gke_node_pool" {
  name     = "${var.cluster_name}-node-pool"
  location = var.zone # ✅ No more hardcoded region
  cluster  = google_container_cluster.gke_cluster.name
  project  = var.project_id

  autoscaling {
    min_node_count = 1
    max_node_count = 2
  }

  node_config {
    machine_type    = var.machine_type
    disk_size_gb    = 20
    service_account = google_service_account.gke_service_account.email

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }

  management {
    auto_upgrade = true
    auto_repair  = true
  }

  lifecycle {
    ignore_changes = [initial_node_count]
  }
}
