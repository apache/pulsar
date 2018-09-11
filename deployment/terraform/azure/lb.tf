resource "azurerm_public_ip" "proxy" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                         = "${var.pulsar_cluster}-public-ip"
  location                     = "${var.azure_location}"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"
  public_ip_address_allocation = "static"
  domain_name_label            = "${azurerm_resource_group.pulsar_rg.name}"
}

resource "azurerm_lb" "proxy_lb" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"

  location                     = "${var.azure_location}"
  name                         = "${var.pulsar_cluster}-proxy-lb"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"

  frontend_ip_configuration {
    name        = "${var.pulsar_cluster}-lb-frontend-ip"
    subnet_id   = "${azurerm_subnet.pulsar_subnet.id}"
    private_ip_address_allocation = "dynamic"
  }
}

resource "azurerm_lb" "proxy_lb_public" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"

  location                     = "${var.azure_location}"
  name                         = "${var.pulsar_cluster}-proxy-public-lb"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"

  frontend_ip_configuration {
    name                 = "${var.pulsar_cluster}-lb-frontend-public-ip"
    public_ip_address_id = "${azurerm_public_ip.proxy.id}"
  }
}

resource "azurerm_lb_backend_address_pool" "proxy_lb_backend" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                         = "${var.pulsar_cluster}-proxy-lb-backend"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"
  loadbalancer_id              = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
}

resource "azurerm_lb_probe" "proxy-httpprobe" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                         = "${var.pulsar_cluster}-proxy-lb-httpprobe"
  port                         = 8080
  protocol                     = "Http"
  request_path                 = "/admin/v2/clusters"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"
  loadbalancer_id              = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
}

resource "azurerm_lb_probe" "proxy-tcpprobe" {
  count                        = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                         = "${var.pulsar_cluster}-proxy-lb-tcpprobe"
  port                         = 6650
  protocol                     = "Tcp"
  resource_group_name          = "${azurerm_resource_group.pulsar_rg.name}"
  loadbalancer_id              = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
}

resource "azurerm_lb_rule" "proxy-lb-rule-http" {
  count                          = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                           = "${var.pulsar_cluster}-proxy-lb-rule-http"
  backend_port                   = 8080
  frontend_port                  = 8080
  frontend_ip_configuration_name = "${var.associate_public_ip == true ? "${var.pulsar_cluster}-lb-frontend-public-ip" : "${var.pulsar_cluster}-lb-frontend-ip"}"
  backend_address_pool_id        = "${azurerm_lb_backend_address_pool.proxy_lb_backend.id}"
  protocol                       = "Tcp"
  loadbalancer_id                = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
  resource_group_name            = "${azurerm_resource_group.pulsar_rg.name}"
}

resource "azurerm_lb_rule" "proxy-lb-rule-protobuf" {
  count                          = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                           = "${var.pulsar_cluster}-proxy-lb-rule-protobuf"
  backend_port                   = 6650
  frontend_port                  = 6650
  frontend_ip_configuration_name = "${var.associate_public_ip == true ? "${var.pulsar_cluster}-lb-frontend-public-ip" : "${var.pulsar_cluster}-lb-frontend-ip"}"
  backend_address_pool_id        = "${azurerm_lb_backend_address_pool.proxy_lb_backend.id}"
  protocol                       = "Tcp"
  loadbalancer_id                = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
  resource_group_name            = "${azurerm_resource_group.pulsar_rg.name}"
}

// SSH access
resource "azurerm_lb_rule" "proxy-lb-rule-ssh" {
  count                          = "${var.associate_public_ip == "true" && var.proxy_count != "0" ? "1" : "0"}"
  name                           = "${var.pulsar_cluster}-proxy-lb-rule-ssh"
  backend_port                   = 22
  frontend_port                  = 22
  frontend_ip_configuration_name = "${var.associate_public_ip == true ? "${var.pulsar_cluster}-lb-frontend-public-ip" : "${var.pulsar_cluster}-lb-frontend-ip"}"
  backend_address_pool_id        = "${azurerm_lb_backend_address_pool.proxy_lb_backend.id}"
  protocol                       = "Tcp"
  loadbalancer_id                = "${var.associate_public_ip == true ? azurerm_lb.proxy_lb_public.id : azurerm_lb.proxy_lb.id}"
  resource_group_name            = "${azurerm_resource_group.pulsar_rg.name}"
}
