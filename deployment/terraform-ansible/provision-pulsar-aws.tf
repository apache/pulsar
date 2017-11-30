provider "aws" {
  region = "${var.region}"
}

# Create a VPC to launch our instances into
resource "aws_vpc" "pulsar_vpc" {
  cidr_block = "10.0.0.0/16"

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
    Id  = "${count.index + 1}"
  }

  provisioner "file" {
    source      = "scripts/install-zookeeper.bash"
    destination = "/tmp/install-zookeeper.bash"

    connection {
      type        = "ssh"
      user        = "ec2-user"
      agent       = false
      private_key = "${file("${var.private_key_path}")}"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo yum install wget java sysstat vim",
      "sudo chmod +x /tmp/install-zookeeper.bash",
      "/tmp/install-zookeeper.bash ${var.versions["zookeeper"]} ${count.index + 1}"
    ]

    connection {
      type        = "ssh"
      user        = "ec2-user"
      agent       = false
      private_key = "${file("${var.private_key_path}")}"
    }
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

  provisioner "file" {
    source      = "scripts/prepare-mounts.bash"
    destination = "/tmp/prepare-mounts.bash"
  }

  provisioner "file" {
    source      = "scripts/install-pulsar.bash"
    destination = "/tmp/install-pulsar.bash"
  }

  provisioner "file" {
    content     = "${data.template_file.bookkeeper_conf.rendered}"
    destination = "/opt/pulsar/conf/bookkeeper.conf"
  }

  provisioner "file" {
    source      = "templates/bookkeeper.service"
    destination = "/etc/systemd/system/bookkeeper.service"
  }

  provisioner "file" {
    source      = "${data.template_file.broker_conf.rendered}"
    destination = "/opt/pulsar/conf/broker.conf"
  }

  provisioner "file" {
    content     = "${count.index + 1}"
    destination = "/opt/pulsar/data/zookeeper/myid"
  }

  provisioner "file" {
    source      = "${data.template_file.pulsar_env_sh_pulsar.rendered}"
    destination = "/opt/pulsar/conf/pulsar_env.sh"
  }

  provisioner "file" {
    source      = "templates/pulsar.service"
    destination = "/etc/systemd/system/pulsar.service"
  }

  provisioner "file" {
    source      = "templates/zookeeper.service"
    destination = "/etc/systemd/system/zookeeper.service"
  }

  provisioner "file" {
    source      = "${data.template_file.zoo_cfg.rendered}"
    destination = "/opt/pulsar/conf/zookeeper.conf"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo yum install wget java sysstat vim",
      "sudo chmod +x /tmp/prepare-mounts.bash",
      "sudo chmod +x /tmp/install-pulsar.bash",
      "/tmp/prepare-mounts.bash",
      "/tmp/install-pulsar.bash ${var.versions["pulsar"]} ${count.index + 1} ${aws_instance.zookeeper.0.private_ip}",
      "sudo systemctl start /etc/systemd/system/zookeeper.service"
    ]

    connection {
      type = "ssh"
      user = "ec2-user"
      agent = false
      private_key = "${file("${var.private_key_path}")}"
    }
  }
}

data "template_file" "zoo_cfg" {
  count = "${var.num_zookeeper_nodes}"
  template = "${file("${path.module}/templates/zoo.cfg")}"

  vars {
    zookeeper_servers = "${join("\n", formatlist("server.%s=%v:2888:3888", aws_instance.zookeeper.*.tags.Id, aws_instance.zookeeper.*.private_ip))}"
  }
}