output "pulsar_connection_url" {
  value = "pulsar://${aws_elb.default.dns_name}:6650"
}

output "pulsar_ssh_hosts" {
  value = "${aws_instance.pulsar.*.public_ip}"
}