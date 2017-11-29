variable "public_key_path" {
  default = "~/.ssh/pulsar_terraform.pub"

  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.

Example: ~/.ssh/my_keys.pub
Default: ~/.ssh/pulsar_terraform.pub
DESCRIPTION
}

variable "private_key_path" {
  default = "~/.ssh/pulsar_terraform"
}

variable "key_name" {
  default     = "pulsar-terraform-keys"
  description = "The name for the AWS key pair to be used for SSH connections"
}

variable "region" {
  default = "us-west-2"
  description = "The AWS region in which the Pulsar cluster will be deployed"
}

variable "ami" {
  default = "ami-9fa343e7" // RHEL-7.4
}

variable "num_zookeeper_nodes" {
  default = 3
}

variable "num_pulsar_brokers" {
  default = 3
}

variable "zookeeper_node_instance_type" {
  default = "t2.small"
}

variable "pulsar_broker_instance_type" {
  default = "i3.4xlarge"
}