output "pulsar_connection_url" {
  value = "pulsar://${aws_instance.zookeeper.0.public_ip}:6650"
}