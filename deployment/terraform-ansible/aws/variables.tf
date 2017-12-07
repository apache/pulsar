variable "public_key_path" {
  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.

Example: ~/.ssh/my_keys.pub
Default: ~/.ssh/id_rsa.pub
DESCRIPTION
}

variable "key_name" {
  description = "The name for the AWS key pair to be used for SSH connections"
  default     = "pulsar-terraform-ssh-keys"
}

variable "region" {
  description = "The AWS region in which the Pulsar cluster will be deployed"
}

variable "availability_zone" {
  description = "The AWS availability zone in which the cluster will run"
}

variable "ami" {
  description = "The AWS AMI to be used by the Pulsar cluster"
}

variable "num_zookeeper_nodes" {
  description = "The number of EC2 instances running ZooKeeper"
}

variable "num_pulsar_brokers" {
  description = "The number of EC2 instances running a Pulsar broker plus a BookKeeper bookie"
}

variable "instance_types" {
  type = "map"
}

variable "base_cidr_block" {
  description = "The baseline CIDR block to be used by network assets for the Pulsar cluster"
}