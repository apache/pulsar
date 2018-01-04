output "dns_name" {
  value = "${aws_elb.default.dns_name}"
}

output "pulsar_service_url" {
  value = "pulsar://${aws_elb.default.dns_name}:6650"
}

output "pulsar_web_url" {
  value = "http://${aws_elb.default.dns_name}:8080"
}

output "pulsar_ssh_host" {
  value = "${aws_instance.pulsar.0.public_ip}"
}