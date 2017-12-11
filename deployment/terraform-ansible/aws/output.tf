output "pulsar_connection_url" {
  value = "pulsar://${aws_elb.default.dns_name}:6650"
}

output "pulsar_ssh_host" {
  value = "${aws_instance.pulsar.0.public_ip}"
}