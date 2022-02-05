locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
}

variable "region" {
    description = "Region for GCP resources. Chose as per your location:  https://cloud.google.com/about/locations"
    default = "asia-south1"
    type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
    description = "Storage class type for your bucket. Check official docs for more info."
    type = string
    default = "trips_data_all"
}