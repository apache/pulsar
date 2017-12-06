output "pulsar_connection_url" {
  value = "pulsar://${aws_elb.default.dns_name}:6650"
}