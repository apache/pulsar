package org.apache.pulsar.proxy.util;

import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

@Data
@AllArgsConstructor
public class ProxyServiceInfo {

    private ProxyService proxyService;

    private ProxyConfiguration proxyConfiguration;

    private InetSocketAddress proxyAddress;
}
