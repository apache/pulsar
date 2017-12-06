resource "aws_instance" "zookeeper" {
  ami                    = "${var.ami}"
  instance_type          = "${var.instance_types["zookeeper"]}"
  key_name               = "${aws_key_pair.default.id}"
  subnet_id              = "${aws_subnet.default.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  count                  = "${var.num_zookeeper_nodes}"

  tags {
    Name = "zookeeper-${count.index + 1}"
  }
}

resource "aws_instance" "pulsar" {
  ami                         = "${var.ami}"
  instance_type               = "${var.instance_types["pulsar"]}"
  key_name                    = "${aws_key_pair.default.id}"
  subnet_id                   = "${aws_subnet.default.id}"
  vpc_security_group_ids      = ["${aws_security_group.default.id}"]
  count                       = "${var.num_pulsar_brokers}"

  tags {
    Name = "pulsar-${count.index + 1}"
  }

  associate_public_ip_address = true
}
