variable "public_key_path" {
  default = "~/.ssh/id_rsa.pub"

  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.

Example: ~/.ssh/my_keys.pub
Default: ~/.ssh/id_rsa.pub
DESCRIPTION
}

variable "key_name" {
  default     = "pulsar-terraform-ssh-keys"
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

variable "instance_types" {
  type = "map"
  default = {
    "pulsar"   = "i3.4xlarge"
    "zookeeper" = "t2.small"
  }
}

variable "subnet_availability_zone" {
  default = "us-west-2a"
}

variable "vpc_cidr_block" {
  default = "10.0.0.0/16"
}