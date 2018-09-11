output "pulsar_public_dns" {
  value = "${azurerm_public_ip.proxy.*.fqdn}"
}

output "pulsar_public_ip_address" {
  value = "${azurerm_public_ip.proxy.*.ip_address}"
}

output "vm_password" {
  value = "${random_string.vm-login-password.result}"
}
