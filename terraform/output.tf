output "gke_service_account_email" {
  value       = google_service_account.gke_service_account.email
  description = "The email address of the GKE service account."
}
