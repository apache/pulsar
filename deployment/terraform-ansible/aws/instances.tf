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

resource "aws_instance" "zookeeper" {
  ami                    = "${var.aws_ami}"
  instance_type          = "${var.instance_types["zookeeper"]}"
  key_name               = "${aws_key_pair.default.id}"
  subnet_id              = "${aws_subnet.default.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  count                  = "${var.num_zookeeper_nodes}"

  tags {
    Name = "zookeeper-${count.index + 1}"
  }
}

resource "aws_instance" "bookie" {
  ami                    = "${var.aws_ami}"
  instance_type          = "${var.instance_types["bookie"]}"
  key_name               = "${aws_key_pair.default.id}"
  subnet_id              = "${aws_subnet.default.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  count                  = "${var.num_bookie_nodes}"

  tags {
    Name = "bookie-${count.index + 1}"
  }
}

resource "aws_instance" "broker" {
  ami                    = "${var.aws_ami}"
  instance_type          = "${var.instance_types["broker"]}"
  key_name               = "${aws_key_pair.default.id}"
  subnet_id              = "${aws_subnet.default.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  count                  = "${var.num_broker_nodes}"

  tags {
    Name = "broker-${count.index + 1}"
  }
}

resource "aws_instance" "proxy" {
  ami                    = "${var.aws_ami}"
  instance_type          = "${var.instance_types["proxy"]}"
  key_name               = "${aws_key_pair.default.id}"
  subnet_id              = "${aws_subnet.default.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  count                  = "${var.num_proxy_nodes}"

  tags {
    Name = "proxy-${count.index + 1}"
  }
}
