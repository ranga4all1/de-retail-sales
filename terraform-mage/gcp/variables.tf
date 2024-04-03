variable "credentials" {
  description = "my credentials"
  default     = "/workspaces/de-retail-sales/creds/my-creds.json"
  # Do NOT upload credentials files to github to repository or anywhere on internet
  # This can be done by adding those files/folders to .gitignore
}
variable "app_name" {
  type        = string
  description = "Application Name"
  default     = "mage-data-prep"
}

variable "container_cpu" {
  description = "Container cpu"
  default     = "2000m"
}

variable "container_memory" {
  description = "Container memory"
  default     = "2G"
}

variable "project_id" {
  type        = string
  description = "The ID of the project"
  default     = "project_id"
}

variable "region" {
  type        = string
  description = "The default compute region"
  default     = "REGION"
}

variable "zone" {
  type        = string
  description = "The default compute zone"
  default     = "ZONE"
}

variable "repository" {
  type        = string
  description = "The name of the Artifact Registry repository to be created"
  default     = "mage-data-prep"
}

variable "database_user" {
  type        = string
  description = "The username of the Postgres database."
  default     = "mageuser"
}

variable "database_password" {
  type        = string
  description = "The password of the Postgres database."
  sensitive   = true
}

variable "docker_image" {
  type        = string
  description = "The docker image to deploy to Cloud Run."
  default     = "mageai/mageai:latest"
}

variable "domain" {
  description = "Domain name to run the load balancer on. Used if `ssl` is `true`."
  type        = string
  default     = ""
}

variable "ssl" {
  description = "Run load balancer on HTTPS and provision managed certificate with provided `domain`."
  type        = bool
  default     = false
}
