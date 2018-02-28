public_key_path     = "~/.ssh/id_rsa.pub"
region              = "us-west-2"
availability_zone   = "us-west-2a"
aws_ami             = "ami-9fa343e7"
num_zookeeper_nodes = 3
num_pulsar_brokers  = 3
base_cidr_block     = "10.0.0.0/16"

instance_types      = {
  "pulsar"    = "i3.xlarge"
  "zookeeper" = "t2.small"
}
