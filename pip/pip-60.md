# PIP-60: Support Proxy server with SNI routing

* **Status**: Discussion
 * **Author**: [Rajan Dhabalia](https://github.com/rdhabalia)
 * **Pull Request**: [#6566](https://github.com/apache/pulsar/pull/6566)
 * **Release**: 2.6.0
## Motivation

A proxy server is a go‑between or intermediary server that forwards requests from multiple clients to different servers across the Internet.  The proxy server can act as a “traffic cop,” in both forward and reverse proxy scenarios, and adds various benefits in your system such as load balancing, performance, security, auto-scaling, etc.. There are already many proxy servers already available in the market which are fast, scalable and more importantly covers various essential security aspects that are needed by the large organization to securely share their confidential data over the network. Pulsar already provides proxy implementation [PIP-1](https://github.com/apache/pulsar/wiki/PIP-1:-Pulsar-Proxy)  which acts as a reverse proxy and creates a gateway in front of brokers. However, pulsar doesn’t provide support to use other proxies such as Apache traffic server (ATS), HAProxy, Nginx, Envoy those are more scalable and secured. Most of these proxy-servers support SNI ROUTING which can route traffic to a destination without having to terminate the SSL connection. Routing at layer 4 gives greater transparency because the outbound connection is determined by examining the destination address in the client TCP packets.  

## SNI Routing

[TLS Extension Definition](https://tools.ietf.org/html/rfc6066) adds support of Server Name Indication [SNI](https://tools.ietf.org/html/rfc6066#page-6) and it has been available since January 2011. In SNI routing, the proxy server examines TLS connection on an initial handshake and extracts the "SNI" value. 
Proxy-server creates a connection with a destination host defined into SNI and forms a TLS tunnel between client and destination host.
Figure 1: shows the layer-4 routing network activity diagram between client and application server via proxy-server (eg: ATS). In the figure, the client initiates TLS connection with the proxy-server by passing hostname of application-server in the SNI header. The proxy server examines SNI header, parses hostname of the target application server and creates an outbound connection with that application server host. Once, proxy server successfully completes TLS handshake with the application-server, the proxy creates a TLS tunnel between the client and the application server host to begin further data transactions. Many known proxy server solutions such as [ATS](https://docs.trafficserver.apache.org/en/latest/admin-guide/layer-4-routing.en.html), [Fabio](https://fabiolb.net/feature/tcp-sni-proxy/), [Envoy](https://www.envoyproxy.io/docs/envoy/latest/configuration/listeners/listener_filters/tls_inspector#config-listener-filters-tls-inspector), [Nginx](https://nginx.org/en/docs/stream/ngx_stream_ssl_preread_module.html), [HaProxy](https://stuff-things.net/2016/11/30/haproxy-sni) support SNI routing to create TLS tunnel between client and server host.


![image](https://user-images.githubusercontent.com/2898254/76898266-9b47c380-6852-11ea-8969-55783cda5ed0.png)
                                                        [Figure 1: Layer 4 routing]

## Pulsar and Proxy with SNI routing

Pulsar client creates TCP connection with the broker and exchanges binary data over TCP channel. Pulsar also supports TLS to create a secure connection between a client and the broker. As we have discussed in figure-1, the proxy server performs SNI routing on TLS connection and this mechanism can be perfectly fit into pulsar to introduce proxy between client and broker, and use proxy-server for both forward-proxy and reverse-proxy use cases. 
In order to access a specific topic, the pulsar-client first finds out the target broker which owns the topic and then creates a connection channel with that target broker to publish/consume messages on that topic. This section explains how pulsar-client uses proxy-server to find a target broker for a specific topic and create a TCP channel with that target broker to access the topic.
Pulsar client connects to the broker in two phases: lookup and connect.  
In lookup phase, a client does a lookup with the broker-service URL / VIP to connect with any broker in the cluster, send lookup request for a topic and receive the response with the name of the target broker that owns the topic. In the second phase, the client uses the lookup response which has the name of the target broker and connects with that target broker to publish/consume messages. 

Figure 2 shows SNI routing for both the phases and how proxy-server creates a TCP tunnel between client and broker. In SNI Routing, pulsar-client always tries to create a connection with proxy-server and pass the target broker/service URL as part of the SNI header. 
 

![image](https://user-images.githubusercontent.com/2898254/76898221-879c5d00-6852-11ea-97ed-155e78f7dead.png)
                                                        [Figure 2: Pulsar-client and broker connection using SNI Routing]

**Lookup phase**

Figure 2 shows that pulsar-client uses broker-service url (pulsar-vip) and proxy-server url (proxy-server:4443) to perform lookup using SNI routing. Pulsar-client uses proxy-server url to create a TLS connection with proxy-server and uses broker-service url to pass it in SNI header. At the time of TLS handshake, Proxy-server does SNI-routing by extracting broker-service name value from SNI header, finds appropriate mapped broker url from configured SNI mapping and connect with a broker to form a TCP tunnel between client and broker. Once, the TCP tunnel is established between pulsar client and broker, the client requests lookup for a specific topic and receives a lookup response with the name of target broker which owns the topic.

**Connect phase**

Once the client receives a lookup response with the target broker which owns the topic, the client requests proxy-server to create a TCP tunnel between client and broker. In order to create a TCP tunnel between client and target broker, the client always creates a connection with proxy-server and passes the target broker name into SNI header. Once the TCP tunnel is created, the client can communicate with the broker and publish/consume messages on that TCP connection.


### Changes

In order to support SNI routing using Proxy-server, we have to mainly make changes into pulsar-client.
#### Pulsar client:
In order to add support of proxy-server into a pulsar, pulsar-client requires knowledge of proxy location and type of proxy routing (eg: SNI). So, we will add additional configuration in pulsar-client:
```
String proxyServiceUrl;
ProxyProtocol proxyProtocol; //eg: enum: ProxyProtocol.SNI 
                             // allows adding more proxy options in configurations
```

#### Geo-replication
We can also use proxy-server in geo-replication to create a proxy between two brokers. Therefore, we need similar configurations as pulsar-client into cluster metadata as well. If cluster metadata has a proxy configured then other clusters will connect to this pulsar cluster via proxy. We can configure cluster-metadata with proxy-uri and protocol  using admin api/cli.
```
./pulsar-admin clusters create my-DMZ-cluster \
--url http://my-dmz-broker.com:8080 \
--broker-url-secure pulsar+ssl://my-dmz-broker.com:6651 \
--proxy-url pulsar+ssl://my-dmz-proxy.com:4443 \
--proxy-protocol SNI
```

## Example
This section shows SNI-routing using ATS-proxy server. This section shows how to configure ATS proxy-server and pulsar-client to create TCP tunnel between client and broker.

![image](https://user-images.githubusercontent.com/2898254/78093926-21eabd80-7388-11ea-8982-4a4d644a2b39.png)
                                                        [Figure 3: Pulsar SNI Routing with ATS proxy server]

In this example, Pulsar broker cluster is behind the ATS proxy. Pulsar brokers can’t be accessed by any host except ATS proxy. So, if client wants to connect to pulsar-cluster then client can use ATS-proxy server to create a TCP tunnel with brokers. Pulsar client can use SNI-routing proxy protocol to connect to ATS-proxy and asks ATS-proxy to create TCP tunnel with a target broker.

This example shows, how can we configure ATS-proxy so, when client passes target broker name into SNI header then ATS-proxy server can find out appropriate broker-url based on configured sni-mapping and forward request to appropriate target broker by creating tcp tunnel with that broker.

### ATS Configuration
In order to enable SNI routing into ATS proxy, we need to manage 2 configuration files:
1. [ssl_server_name.config](https://docs.trafficserver.apache.org/en/8.0.x/admin-guide/files/ssl_server_name.yaml.en.html)

This file is used to configure aspects of TLS connection handling for both inbound and outbound connections. The configuration is driven by the SNI values provided by the inbound connection. So, this file manages mapping between hostname which comes into SNI header and actual broker-url where request needs to be forwarded for that host.   
ssl_server_name.config

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

2.[records.config](https://docs.trafficserver.apache.org/en/8.0.x/admin-guide/files/records.config.en.html)

The records.config file, located in /usr/local/etc/trafficserver/) is a list of configurable variables used by the Traffic Server software. One of the requirements of SNI routing in ATS is that it only works over TLS. Therefore, Pulsar brokers and ATS proxy-server should have tls enabled. We will define tls configuration for ATS-proxy server into records.config file.

```
CONFIG proxy.config.http.connect_ports STRING 4443 6651
# ats-proxy cert file
CONFIG proxy.config.ssl.client.cert.path STRING /ats-cert.pem
# ats-proxy key file
CONFIG proxy.config.ssl.client.cert.filename STRING /ats-key.pem
# ssl-port on which ats will listen
CONFIG proxy.config.http.server_ports STRING 4443:ssl 4080
```
Once, `ssl_server_name.config` and `records.config` are configured, ATS-proxy server is ready to handle SNI routing and can create TCP tunnel beween client and broker.

### Pulsar-client Configuration
Now, ATS proxy server is configured and ready to handle SNI routing and create TCP tunnel between client and broker. With this PIP, pulsar-client supports SNI routing by connecting to proxy and sending target broker url into SNI header. Pulsar-client handles SNI routing internally and entire connection handling is abstracted from user. User have to only configure following proxy configuration intially when user creates a pulsar-client to use SNI routing protocol.
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
