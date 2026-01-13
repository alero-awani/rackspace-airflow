terraform {
  required_providers {
    spot = {
      source = "rackerlabs/spot"
    }
  }
}

provider "spot" {
  token = var.rackspace_spot_token
}

resource "spot_cloudspace" "airflow-cluster" {
  cloudspace_name    = "airflow-cluster"
  region             = "us-central-dfw-1"
  hacontrol_plane    = false
  wait_until_ready   = true
  kubernetes_version = "1.31.1"
  cni                = "calico"
}

resource "spot_spotnodepool" "workers" {
  cloudspace_name      = spot_cloudspace.airflow-cluster.cloudspace_name
  server_class         = "mh.vs1.medium-dfw"
  bid_price            = 0.09
  desired_server_count = 2
  # autoscaling = {
  #   min_nodes = 2
  #   max_nodes = 4
  # }

  labels = {
    "managed-by" = "terraform"
  }
}