# ===================================================================
# GOOGLE SERVICE ACCOUNT OLUŞTURMA
# ===================================================================
resource "google_service_account" "gke_service_account" {
  project      = var.project_id
  account_id   = var.account_id
  display_name = var.description

  depends_on = [google_project_service.enabled_apis]

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [display_name]
  }
}

# ===================================================================
# SERVICE ACCOUNT'A IAM ROLLERİNİ ATAMA
# ===================================================================
resource "google_project_iam_member" "gke_sa_iam" {
  for_each = toset(var.roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_service_account.email}"

  depends_on = [google_service_account.gke_service_account]
}

# ===================================================================
# SERVICE ACCOUNT KEY OLUŞTURMA
# ===================================================================
resource "google_service_account_key" "gke_sa_key" {
  service_account_id = google_service_account.gke_service_account.name
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"

  depends_on = [google_service_account.gke_service_account]
}

# ===================================================================
# SECRET MANAGER'DA SECRET OLUŞTURMA
# ===================================================================
resource "google_secret_manager_secret" "gke_sa_secret" {
  secret_id = "gke-service-account-key"
  project   = var.project_id

  replication {
    auto {}
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [] # optionally allow metadata updates
  }

  depends_on = [google_project_service.enabled_apis]
}

# ===================================================================
# SECRET VERSION YÜKLEME (SERVICE ACCOUNT KEY)
# ===================================================================
resource "google_secret_manager_secret_version" "gke_sa_secret_version" {
  secret      = google_secret_manager_secret.gke_sa_secret.id
  secret_data = google_service_account_key.gke_sa_key.private_key

  depends_on = [
    google_secret_manager_secret.gke_sa_secret,
    google_service_account_key.gke_sa_key
  ]

  lifecycle {
    ignore_changes = [secret_data]
  }
}
