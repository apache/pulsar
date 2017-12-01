# Create a VPC to launch our instances into
resource "aws_vpc" "pulsar_vpc" {
  cidr_block           = "10.0.0.0/16"
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

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.pulsar_vpc.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.default.id}"
}

/*
resource "aws_elb" "pulsar_elb" {
  name                        = "pulsar-load-balancer"
  availability_zones          = ["${var.region}"]
  instances                   = ["${aws_instance.pulsar.*.tags.Name}"]
  security_groups             = ["${aws_security_group.pulsar_security_group.id}"]
  cross_zone_load_balancing   = false
  idle_timeout                = 400
  connection_draining         = true
  connection_draining_timeout = 400

  listener {
    instance_port     = 8080
    instance_protocol = "http"
    lb_port           = 80
    lb_protocol       = "http"
  }

  tags {
    Name = "Pulsar-ELB"
  }
}

resource "aws_route53_zone" "pulsar_zone" {
  name = "${var.dns_name}"
}

resource "aws_route53_record" "pulsar_dns" {
  zone_id = "${aws_route53_zone.pulsar_zone.zone_id}"
  count   = "${var.num_pulsar_brokers}"
  name    = "pulsar-${count.index}.${var.dns_name}"
  type    = "CNAME"
  ttl     = "300"
  records = ["${aws_elb.pulsar_elb.dns_name}"]
}
*/

# Create a subnet to launch our instances into
resource "aws_subnet" "pulsar_subnet" {
  vpc_id                  = "${aws_vpc.pulsar_vpc.id}"
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true
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