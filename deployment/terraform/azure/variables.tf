variable "azure_location" {
  type = "string"
  default = "eastus"
}

variable "pulsar_version" {
  description = "Pulsar version"
  default = "2.1.0-incubating"
}

variable "pulsar_cluster" {
  description = "Name of the pulsar cluster, used in node discovery"
  default = "pulsar-staging"
}

variable "public_key_path" {
  description = "Key name to be used with the launched vms."
  default = "~/.ssh/id_rsa.pub"
}

variable "environment" {
  default = "staging"
}

variable "bk_instance_type" {
  type = "string"
  default = "Standard_D12_v2"
}

variable "broker_instance_type" {
  type = "string"
  default = "Standard_D12_v2"
}

variable "zk_instance_type" {
  type = "string"
  default = "Standard_A2_v2"
}

variable "proxy_instance_type" {
  type = "string"
  default = "Standard_A2_v2"
}

variable "journal_volume_size" {
  type = "string"
  default = "10" # gb
}

variable "storage_volume_size" {
  type = "string"
  default = "50" # gb
}

variable "use_instance_storage" {
  default = "true"
}

variable "associate_public_ip" {
  default = "true"
}

variable "pulsar_data_dir" {
  default = "/mnt/pulsar/data"
}

variable "pulsar_logs_dir" {
  default = "/var/log/pulsar"
}

variable "zk_count" {
  default = "3"
}

variable "bk_count" {
  default = "3"
}

variable "broker_count" {
  default = "3"
}

variable "proxy_count" {
  default = "3"
}
