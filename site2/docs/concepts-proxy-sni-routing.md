---
id: concepts-proxy-sni-routing
title: Proxy support with SNI routing
sidebar_label: Proxy support with SNI routing
---

# Pulsar Proxy with SNI routing

A proxy server is a go‑between or intermediary server that forwards requests from multiple clients to different servers across the Internet. The proxy server can act as a “traffic cop,” in both forward and reverse proxy scenarios, and adds various benefits in your system such as load balancing, performance, security, auto-scaling, etc.. There are already many proxy servers already available in the market which are fast, scalable and more importantly covers various essential security aspects that are needed by the large organization to securely share their confidential data over the network. Pulsar already provides proxy implementation which acts as a reverse proxy and creates a gateway in front of brokers. However, pulsar doesn’t provide support to use other proxies such as Apache traffic server (ATS), HAProxy, Nginx, Envoy those are more scalable and secured. Most of these proxy-servers support SNI ROUTING which can route traffic to a destination without having to terminate the SSL connection. Routing at layer 4 gives greater transparency because the outbound connection is determined by examining the destination address in the client TCP packets.

[PIP-60](https://github.com/apache/pulsar/wiki/PIP-60:-Support-Proxy-server-with-SNI-routing) eexplains SNI routing protocol and how Pulsar clients support SNI routing protocol to connect to brokers via proxy. This document explains how to setup ATS proxy and pulsar-client to enable SNI routing and connect pulsar-client to pulsar-broker via ATS proxy.

## ATS-SNI Routing in Pulsar
[ATS supports layer-4 SNI routing](https://docs.trafficserver.apache.org/en/latest/admin-guide/layer-4-routing.en.html) with requirement that inbound connection must be TLS. Pulsar client supports SNI routing protocol and we can use ATS proxy in front of brokers, so pulsar client can connect to broker via ATS proxy. Pulsar also supports SNI routing for geo-replication, so brokers can connect to cross cluster brokers via ATS proxy.

### ATS Proxy setup for layer-4 SNI routing

This section explains ATS-setup to enable layer-4 SNI routing and use ATS proxy as a forward proxy where the pulsar broker runs behind the ATS proxy and client connects to pulsar-broker through this ATS proxy.


![Snip20200701_37](https://user-images.githubusercontent.com/2898254/86283497-09ac5980-bb96-11ea-8b2b-45351977bd55.png)

We have to configure two conf files into ATS proxy to support SNI routing.


1. `records.conf`: 
The [records.config fil](https://docs.trafficserver.apache.org/en/latest/admin-guide/files/records.config.en.html) (by default, located in `/usr/local/etc/trafficserver/`) is a list of configurable variables used by the Traffic Server and we have to update this file with tls port (`http.server_ports`) on which proxy can listen and proxy certs (`ssl.client.cert.path` and `ssl.client.cert.filename`) for secure tls tunneling. We also have to configure a range of server ports (`http.connect_ports`) that can be used for tunneling to pulsar-broker. If Pulsar brokers are listening on 4443 and 6651 then add brokers’ service port in http.connect_ports configuration.

```
# PROXY TLS PORT
CONFIG proxy.config.http.server_ports STRING 4443:ssl 4080
# PROXY CERTS FILE PATH
CONFIG proxy.config.ssl.client.cert.path STRING /proxy-cert.pem
# PROXY KEY FILE PATH
CONFIG proxy.config.ssl.client.cert.filename STRING /proxy-key.pem


# The range of origin server ports that can be used for tunneling via CONNECT. # Traffic Server allows tunnels only to the specified ports. Supports both wildcards (*) and ranges (e.g. 0-1023).
CONFIG proxy.config.http.connect_ports STRING 4443 6651
```

2. `ssl_server_name.conf`: 
The [ssl_server_name file](https://docs.trafficserver.apache.org/en/8.0.x/admin-guide/files/ssl_server_name.yaml.en.html) is used to configure aspects of TLS connection handling for both inbound and outbound connections. The configuration is driven by the SNI values provided by the inbound connection. The file consists of a set of configuration items, each identified by an SNI value (`fqdn`). When an inbound TLS connection is made, the SNI value from the TLS negotiation is matched against the items specified by this file and if there is a match, the values specified in that item override the defaults. 
Below example of  `ssl_server_name`shows mapping of inbound SNI hostname coming from client and actual broker’s service url where request should be redirected. For example: if client sends SNI header `pulsar-broker1` then proxy creates tls tunnel by redirecting request to service-url: `pulsar-broker1:6651` 

```
server_config = {
  {
     fqdn = 'pulsar-broker-vip',
     # Forward to Pulsar broker which is listening on 6651
     tunnel_route = 'pulsar-broker-vip:6651'
  },
  {
     fqdn = 'pulsar-broker1',
     # Forward to Pulsar broker-1 which is listening on 6651
     tunnel_route = 'pulsar-broker1:6651'
  },
  {
     fqdn = 'pulsar-broker2',
     # Forward to Pulsar broker-2 which is listening on 6651
     tunnel_route = 'pulsar-broker2:6651'
  },
}
```
Once, ssl_server_name.config and records.config are configured, ATS-proxy server is ready to handle SNI routing and can create TCP tunnel between client and broker.

### Pulsar-client Configuration with SNI routing

Now, ATS proxy server is configured and ready to handle SNI routing and create TCP tunnel between client and broker. Here, we have to note that ATS SNI-routing works only with TLS. so, ATS proxy and brokers must have tls enabled before Pulsar-client configures SNI routing protocol to connect to broker via ATS proxy. With [PIP-60](https://github.com/apache/pulsar/wiki/PIP-60:-Support-Proxy-server-with-SNI-routing), pulsar-client supports SNI routing by connecting to proxy and sending target broker url into SNI header. Pulsar-client handles SNI routing internally and entire connection handling is abstracted from the user. Users have to only configure following proxy configuration initially when the user creates a pulsar-client to use SNI routing protocol.

```
String brokerServiceUrl = “pulsar+ssl://pulsar-broker-vip:6651/”;
String proxyUrl = “pulsar+ssl://ats-proxy:443”;
ClientBuilder clientBuilder = PulsarClient.builder()
		.serviceUrl(brokerServiceUrl)
        .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
        .enableTls(true)
        .allowTlsInsecureConnection(false)
        .proxyServiceUrl(proxyUrl, ProxyProtocol.SNI)
        .operationTimeout(1000, TimeUnit.MILLISECONDS);

Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
clientBuilder.authentication(AuthenticationTls.class.getName(), authParams);

PulsarClient pulsarClient = clientBuilder.build();
```

### Pulsar geo-replication with SNI routing

We can also use ATS proxy for geo-replication. Pulsar broker has capability to connect to cross colo brokers for geo-replication using SNI routing. In order to enable SNI routing for cross cluster broker connection we have to configure SNI proxy URL to cluster metadata. If cluster metadata has SNI proxy url configured then broker will connect to cross cluster broker via proxy over SNI routing.

![Snip20200701_36](https://user-images.githubusercontent.com/2898254/86283369-c94cdb80-bb95-11ea-865e-34ddfe1ad33f.png)

In this example, we have a pulsar cluster deployed into two separate regions, us-west and us-east. We have also configured ATS proxy in both the regions, and brokers in each region running behind this ATS proxy. Now, we will configure cluster metadata for both the clusters, so brokers in one cluster will use SNI routing and connect to brokers in other clusters via ATS proxy.

(a) Configure cluster metadata for us-east with us-east broker service url and us-east ATS proxy url with SNI proxy-protocol.

```
./pulsar-admin clusters update \
--broker-url-secure pulsar+ssl://east-broker-vip:6651 \
--url http://east-broker-vip:8080 \
--proxy-protocol SNI \
--proxy-url pulsar+ssl://east-ats-proxy:443
```

(b) Configure cluster metadata for us-east with us-east broker service url and us-east ATS proxy url with SNI proxy-protocol.

```
./pulsar-admin clusters update \
--broker-url-secure pulsar+ssl://west-broker-vip:6651 \
--url http://west-broker-vip:8080 \
--proxy-protocol SNI \
--proxy-url pulsar+ssl://west-ats-proxy:443
```

