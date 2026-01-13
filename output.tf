data "spot_kubeconfig" "kubeconfig-airflow" {
  cloudspace_name = resource.spot_cloudspace.airflow-cluster.name
}

output "kubeconfig" {
  value = data.spot_kubeconfig.kubeconfig-airflow.raw
}