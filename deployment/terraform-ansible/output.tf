output "pulsar_web_service_url" {
  value = "http://${aws_instance.pulsar.0.public_ip}:8080"
}

output "pulsar_connection_url" {
  value = "pulsar://${aws_instance.pulsar.0.public_ip}:6650"
}

output "pulsar_ssh_host" {
  value = "${aws_instance.pulsar.0.public_ip}"
}