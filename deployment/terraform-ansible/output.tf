output "pulsar_connection_url" {
  value = "pulsar://${aws_instance.pulsar.0.public_ip}:6650"
}