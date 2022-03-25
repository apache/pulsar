#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

variable "public_key_path" {
  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.

Example: ~/.ssh/my_keys.pub
Default: ~/.ssh/id_rsa.pub
DESCRIPTION
}

variable "key_name_prefix" {
  description = "The prefix for the randomly generated name for the AWS key pair to be used for SSH connections (e.g. `pulsar-terraform-ssh-keys-0a1b2cd3`)"
  default     = "pulsar-terraform-ssh-keys"
}

variable "region" {
  description = "The AWS region in which the Pulsar cluster will be deployed"
}

variable "availability_zone" {
  description = "The AWS availability zone in which the cluster will run"
}

variable "aws_ami" {
  description = "The AWS AMI to be used by the Pulsar cluster"
}

variable "num_zookeeper_nodes" {
  description = "The number of EC2 instances running ZooKeeper"
}

variable "num_bookie_nodes" {
  description = "The number of EC2 instances running BookKeeper"
}

variable "num_broker_nodes" {
  description = "The number of EC2 instances running Pulsar brokers"
}

variable "num_proxy_nodes" {
  description = "The number of EC2 instances running Pulsar proxies"
}

variable "instance_types" {
  type = map(string)
}

variable "base_cidr_block" {
  description = "The baseline CIDR block to be used by network assets for the Pulsar cluster"
}
