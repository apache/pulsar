data "template_file" "zk_script" {
  template = "${file("${path.module}/../templates/pulsar.sh")}"

  vars {
  }
}

resource "azurerm_virtual_machine_scale_set" "zk" {
  count               = "${var.zk_count}"
  name                = "${var.pulsar_cluster}-zookeeper"
  location            = "${azurerm_resource_group.pulsar_rg.location}"
  resource_group_name = "${azurerm_resource_group.pulsar_rg.name}"

  sku {
    name = "${var.zk_instance_type}"
    tier = "Standard"
    capacity = "${var.zk_count}"
  }
  upgrade_policy_mode = "Manual"
  overprovision = false

  os_profile {
    computer_name_prefix = "${var.pulsar_cluster}-zk"
    admin_username = "pulsar"
    admin_password = "${random_string.vm-login-password.result}"
    custom_data = "${data.template_file.zk_script.rendered}"
  }

  network_profile {
    name = "${var.pulsar_cluster}-zk-net-profile"
    primary = true

    "ip_configuration" {
      name = "${var.pulsar_cluster}-zk-ip-conf"
      subnet_id = "${azurerm_subnet.pulsar_subnet.id}"
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
