provider "azurerm" {}

resource "random_string" "vm-login-password" {
  length = 32
  special = true
  override_special = "!@#%&-_"
}

resource "azurerm_resource_group" "pulsar_rg" {
  location = "${var.azure_location}"
  name = "${var.pulsar_cluster}-rg"
}

resource "azurerm_virtual_network" "pulsar_vnet" {
  name                = "${var.pulsar_cluster}-vnet"
  location            = "${azurerm_resource_group.pulsar_rg.location}"
  resource_group_name = "${azurerm_resource_group.pulsar_rg.name}"
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "pulsar_subnet" {
  name                 = "${var.pulsar_cluster}-subnet"
  resource_group_name  = "${azurerm_resource_group.pulsar_rg.name}"
  virtual_network_name = "${azurerm_virtual_network.pulsar_vnet.name}"
  address_prefix       = "10.0.1.0/24"
}

resource "azurerm_availability_set" "pulsar_avset" {
  name                = "${var.pulsar_cluster}-avset"
  location            = "${azurerm_resource_group.pulsar_rg.location}"
  resource_group_name = "${azurerm_resource_group.pulsar_rg.name}"
  managed             = true
}
