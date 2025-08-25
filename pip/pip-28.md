# PIP-28: Pulsar Proxy Gateway Improvement

* Status: **Implemented**
* Author: [Samuel Sun](https://github.com/foreversunyao)
* Pull Request: [3915](https://github.com/apache/pulsar/pull/3915)
* Mailing List Discussion: [PIP-28](https://lists.apache.org/thread.html/37d91f72698ef51a0b6cfc65ac82b0e39314cadbb73c4d8ba4d02659@%3Ccommits.pulsar.apache.org%3E)
* Release: 2.4.0

## Motivation

Pulsar Proxy is almost a gateway for all pulsar requests, it could be useful if it can record more details for the traffic, like source, target, session id, response time for each request, even for request message body. For request message body, we need a "switch" to enable/disable this, due to it could make proxy a bit slower.

Currently we have for proxy: 
 - pulsar_proxy_active_connections
 - pulsar_proxy_new_connections
 - pulsar_proxy_rejected_connections
 - pulsar_proxy_binary_ops
 - pulsar_proxy_binary_bytes
 - session_id,source_ip:port,target_ip:port for new connection created by ConnectionPool.java

What we will have in proxy log for each request to Proxy from client:
 - **client_ip:port**
 - **proxy_ip:port**
 - **broker_ip:port**
 - **session_id**
 - **response_time** from proxy sending request to broker to proxy received ack from broker, basically processing time cost, including producer/consumer
 - **topic name**
 - **msg body** could be plain text after decryption
 
And also a new config for enable/disable msg body
- proxyLogLevel in proxy.conf
```
// Proxy log level, default is 0.
// 0: Do not log any tcp channel info
// 1: Parse and log any tcp channel info and command info without message bod
// 2: Parse and log channel info, command info and message body
proxyLogLevel=0
```
Log Example could be:

```
07:27:28.930 [pulsar-discovery-io-2-27] INFO  org.apache.pulsar.proxy.server.ParserProxyHandler - frontendconn:[/clientip:port/proxyip:port] cmd:SEND msg:[11914] my-message

07:50:33.179 [pulsar-discovery-io-2-2] INFO org.apache.pulsar.proxy.server.ParserProxyHandler - backendconn:[/brokerip:port/proxyip:port] cmd:MESSAGE msg:[1396163] msg
```

## code
Potential mainly change these codes:

**ProxyConnection.java and ConnectionPool.java**
main logic for getting client info and keep it proxy, record the response time and get request msg boday when proxy send request to broker.

**AdminProxyHandler.java**
enable/disable msg body output

**ProxyService.java**
new metrics process
