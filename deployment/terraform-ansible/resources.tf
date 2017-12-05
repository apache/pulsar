# Create a VPC to launch our instances into
resource "aws_vpc" "pulsar_vpc" {
  cidr_block           = "${var.vpc_cidr_block}"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags {
    Name = "Pulsar-VPC"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "default" {
  vpc_id = "${aws_vpc.pulsar_vpc.id}"
}

resource "aws_route_table" "default" {
  vpc_id = "${aws_vpc.pulsar_vpc.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.default.id}"
  }
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.pulsar_vpc.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.default.id}"
}

resource "aws_eip" "pulsar_eip" {
  vpc        = true
  depends_on = ["aws_internet_gateway.default"]
}

resource "aws_route_table_association" "pulsar_route_table_association" {
  subnet_id      = "${aws_subnet.pulsar_subnet.id}"
  route_table_id = "${aws_vpc.pulsar_vpc.main_route_table_id}"
}

# Create a subnet to launch our instances into
resource "aws_subnet" "pulsar_subnet" {
  vpc_id                  = "${aws_vpc.pulsar_vpc.id}"
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${var.subnet_availability_zone}"
}

resource "aws_security_group" "pulsar_security_group" {
  name   = "terraform"
  vpc_id = "${aws_vpc.pulsar_vpc.id}"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All ports open within the VPC
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  ingress {
    from_port   = 6650
    to_port     = 6650
    protocol    = "tcp"
    cidr_blocks = ["${var.vpc_cidr_block}"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["${var.vpc_cidr_block}"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags {
    Name = "Pulsar-Security-Group"
  }
}

resource "aws_elb" "load_balancer" {
  name            = "pulsar-lb"
  security_groups = ["${aws_security_group.pulsar_security_group.id}"]
  subnets         = ["${aws_subnet.pulsar_subnet.id}"]
  instances       = ["${aws_instance.pulsar.*.id}"]

  listener {
    instance_port     = 6650
    instance_protocol = "tcp"
    lb_port           = 6650
    lb_protocol       = "tcp"
  }

  listener {
    instance_port     = 8080
    instance_protocol = "http"
    lb_port           = 8080
    lb_protocol       = "http"
  }

  cross_zone_load_balancing = false
}

resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}"
  public_key = "${file(var.public_key_path)}"
}

resource "aws_instance" "zookeeper" {
  ami                    = "${var.ami}"
  instance_type          = "${var.instance_types["zookeeper"]}"
  key_name               = "${aws_key_pair.auth.id}"
  subnet_id              = "${aws_subnet.pulsar_subnet.id}"
  vpc_security_group_ids = ["${aws_security_group.pulsar_security_group.id}"]
  count                  = "${var.num_zookeeper_nodes}"

  tags {
    Name = "zk-${count.index + 1}"
  }
}

resource "aws_instance" "pulsar" {
  ami                    = "${var.ami}"
  instance_type          = "${var.instance_types["pulsar"]}"
  key_name               = "${aws_key_pair.auth.id}"
  subnet_id              = "${aws_subnet.pulsar_subnet.id}"
  vpc_security_group_ids = ["${aws_security_group.pulsar_security_group.id}"]
  count                  = "${var.num_pulsar_brokers}"

  tags {
    Name = "pulsar-${count.index + 1}"
  }
}
