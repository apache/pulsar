
data "template_file" "bookkeeper_conf" {
  template = "${file("${path.module}/templates/bookkeeper.conf")}"

  vars {
    zookeeper_servers = "${join(",", formatlist("%v:2181", aws_instance.zookeeper.*.public_ip))}"
    advertised_address = "${lookup(aws_instance.zookeeper.*.public_ip, )}"
  }
}

data "template_file" "broker_conf" {
  template = "${file("${path.module}/templates/broker.conf")}"

  vars {
    zookeeper_servers = "${join(",", formatlist("%v:2181", aws_instance.zookeeper.*.private_ip))}"
    advertised_address = "${}"
  }
}

data "template_file" "pulsar_env_sh_zookeeper" {
  template = "${file("${path.module}/templates/pulsar_env.sh")}"

  vars {
    max_heap_memory   = "512m"
    max_direct_memory = "512m"
  }
}

data "template_file" "pulsar_env_sh_pulsar" {
  template = "${file("${path.module}/templates/pulsar_env.sh")}"

  vars {
    max_heap_memory   = "24g"
    max_direct_memory = "24g"
  }
}

data "template_file" "zoo_cfg" {
  template = "${file("${path.module}/templates/zoo.cfg")}"

  vars {
    zookeeper_servers = "${join("\n", formatlist("server.%s=%v:2888:3888", aws_instance.zookeeper.*.tags.Id, aws_instance.zookeeper.*.private_ip))}"
  }
}