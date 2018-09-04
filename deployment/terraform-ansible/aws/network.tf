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

resource "aws_vpc" "pulsar_vpc" {
  cidr_block           = "${var.base_cidr_block}"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags {
    Name = "Pulsar-VPC"
  }
}

resource "aws_subnet" "default" {
  vpc_id                  = "${aws_vpc.pulsar_vpc.id}"
  cidr_block              = "${cidrsubnet(var.base_cidr_block, 8, 2)}"
  availability_zone       = "${var.availability_zone}"
  map_public_ip_on_launch = true

  tags {
    Name = "Pulsar-Subnet"
  }
}

resource "aws_route_table" "default" {
  vpc_id = "${aws_vpc.pulsar_vpc.id}"

  tags {
    Name = "Pulsar-Route-Table"
  }
}

resource "aws_route" "default" {
  route_table_id         = "${aws_route_table.default.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.default.id}"
}

resource "aws_route_table_association" "default" {
  subnet_id      = "${aws_subnet.default.id}"
  route_table_id = "${aws_vpc.pulsar_vpc.main_route_table_id}"
}

/* Misc */
resource "aws_eip" "default" {
  vpc        = true
  depends_on = ["aws_internet_gateway.default"]
}

resource "aws_internet_gateway" "default" {
  vpc_id = "${aws_vpc.pulsar_vpc.id}"

  tags {
    Name = "Pulsar-Internet-Gateway"
  }
}

resource "aws_nat_gateway" "default" {
  allocation_id = "${aws_eip.default.id}"
  subnet_id     = "${aws_subnet.default.id}"
  depends_on    = ["aws_internet_gateway.default"]

  tags {
    Name = "Pulsar-NAT-Gateway"
  }
}

/* Public internet route */
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.pulsar_vpc.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.default.id}"
}

/* Load balancer */
resource "aws_elb" "default" {
  name            = "pulsar-elb"
  instances       = ["${aws_instance.proxy.*.id}"]
  security_groups = ["${aws_security_group.elb.id}"]
  subnets         = ["${aws_subnet.default.id}"]

  listener {
    instance_port     = 8080
    instance_protocol = "http"
    lb_port           = 8080
    lb_protocol       = "http"
  }

  listener {
    instance_port     = 6650
    instance_protocol = "tcp"
    lb_port           = 6650
    lb_protocol       = "tcp"
  }

  cross_zone_load_balancing = false

  tags {
    Name = "Pulsar-Load-Balancer"
  }
}
