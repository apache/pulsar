data "template_file" "proxy_script" {
  template = "${file("${path.module}/proxy-init.yml")}"

  vars {
    zk_servers = "${var.pulsar_cluster}-zk000000,${var.pulsar_cluster}-zk000001,${var.pulsar_cluster}-zk000002"
    pulsar_version = "${var.pulsar_version}"
  }
}

data "template_cloudinit_config" "proxy_config" {
  gzip          = true
  base64_encode = true

  part {
    content = "${data.template_file.proxy_script.rendered}"
  }
}

resource "azurerm_virtual_machine_scale_set" "proxy" {
  count               = "${var.proxy_count}"
  name                = "${var.pulsar_cluster}-proxy"
  location            = "${azurerm_resource_group.pulsar_rg.location}"
  resource_group_name = "${azurerm_resource_group.pulsar_rg.name}"

  sku {
    name = "${var.proxy_instance_type}"
    tier = "Standard"
    capacity = "${var.proxy_count}"
  }
  upgrade_policy_mode = "Manual"
  overprovision = false

  os_profile {
    computer_name_prefix = "${var.pulsar_cluster}-proxy"
    admin_username = "pulsar"
    admin_password = "${random_string.vm-login-password.result}"
    custom_data = "${data.template_cloudinit_config.proxy_config.rendered}"
  }

  network_profile {
    name = "${var.pulsar_cluster}-proxy-net-profile"
    primary = true

    "ip_configuration" {
      name = "${var.pulsar_cluster}-proxy-ip-profile"
      subnet_id = "${azurerm_subnet.pulsar_subnet.id}"
      load_balancer_backend_address_pool_ids = ["${azurerm_lb_backend_address_pool.proxy_lb_backend.id}"]
    }
  }

  storage_profile_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "16.04-LTS"
    version   = "latest"
  }

  storage_profile_os_disk {
    caching        = "ReadWrite"
    create_option  = "FromImage"
    managed_disk_type = "Standard_LRS"
  }

  os_profile_linux_config {
    disable_password_authentication = true
    ssh_keys {
      path     = "/home/pulsar/.ssh/authorized_keys"
      key_data = "${file(var.public_key_path)}"
    }
  }
}

