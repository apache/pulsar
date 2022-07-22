# Client
:::note

This page is automatically generated from code files.
If you find something inaccurate, feel free to update `org.apache.pulsar.client.impl.conf.ClientConfigurationData`. Do NOT edit this markdown file manually. Manual changes will be overwritten by automatic generation.

:::
## Optional
### authParamMap
Authentication map of the client.

**Default**: `null`

### authParams
Authentication parameter of the client.

**Default**: `null`

### authPluginClassName
Class name of authentication plugin of the client.

**Default**: `null`

### authentication
Authentication settings of the client.

**Default**: `null`

### concurrentLookupRequest
The number of concurrent lookup requests that can be sent on each broker connection. Setting a maximum prevents overloading a broker.

**Default**: `5000`

### connectionTimeoutMs
Duration of waiting for a connection to a broker to be established.If the duration passes without a response from a broker, the connection attempt is dropped.

**Default**: `10000`

### connectionsPerBroker
Number of connections established between the client and each Broker. A value of 0 means to disable connection pooling.

**Default**: `1`

### dnsLookupBindAddress
The Pulsar client dns lookup bind address, default behavior is bind on 0.0.0.0

**Default**: `null`

### dnsLookupBindPort
The Pulsar client dns lookup bind port, takes effect when dnsLookupBindAddress is configured, default value is 0.

**Default**: `0`

### enableBusyWait
Whether to enable BusyWait for EpollEventLoopGroup.

**Default**: `false`

### enableTransaction
Whether to enable transaction.

**Default**: `false`

### initialBackoffIntervalNanos
Initial backoff interval (in nanosecond).

**Default**: `100000000`

### keepAliveIntervalSeconds
Seconds of keeping alive interval for each client broker connection.

**Default**: `30`

### listenerName
Listener name for lookup. Clients can use listenerName to choose one of the listeners as the service URL to create a connection to the broker as long as the network is accessible."advertisedListeners" must enabled in broker side.

**Default**: `null`

### lookupTimeoutMs
Client lookup timeout (in milliseconds).

**Default**: `-1`

### maxBackoffIntervalNanos
Max backoff interval (in nanosecond).

**Default**: `60000000000`

### maxLookupRedirects
Maximum times of redirected lookup requests.

**Default**: `20`

### maxLookupRequest
Maximum number of lookup requests allowed on each broker connection to prevent overloading a broker.

**Default**: `50000`

### maxNumberOfRejectedRequestPerConnection
Maximum number of rejected requests of a broker in a certain time frame (30 seconds) after the current connection is closed and the client creating a new connection to connect to a different broker.

**Default**: `50`

### memoryLimitBytes
Limit of client memory usage (in byte). The 64M default can guarantee a high producer throughput.

**Default**: `67108864`

### numIoThreads
Number of IO threads.

**Default**: `1`

### numListenerThreads
Number of consumer listener threads.

**Default**: `1`

### operationTimeoutMs
Client operation timeout (in milliseconds).

**Default**: `30000`

### proxyProtocol
Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive.

**Default**: `null`

### proxyServiceUrl
URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive.

**Default**: `null`

### requestTimeoutMs
Maximum duration for completing a request.

**Default**: `60000`

### serviceUrl
Pulsar cluster HTTP URL to connect to a broker.

**Default**: `null`

### serviceUrlProvider
The implementation class of ServiceUrlProvider used to generate ServiceUrl.

**Default**: `null`

### socks5ProxyAddress
Address of SOCKS5 proxy.

**Default**: `null`

### socks5ProxyPassword
Password of SOCKS5 proxy.

**Default**: `null`

### socks5ProxyUsername
User name of SOCKS5 proxy.

**Default**: `null`

### sslProvider
The TLS provider used by an internal client to authenticate with other Pulsar brokers.

**Default**: `null`

### statsIntervalSeconds
Interval to print client stats (in seconds).

**Default**: `60`

### tlsAllowInsecureConnection
Whether the client accepts untrusted TLS certificates from the broker.

**Default**: `false`

### tlsCiphers
Set of TLS Ciphers.

**Default**: `[]`

### tlsHostnameVerificationEnable
Whether the hostname is validated when the proxy creates a TLS connection with brokers.

**Default**: `false`

### tlsProtocols
Protocols of TLS.

**Default**: `[]`

### tlsTrustCertsFilePath
Path to the trusted TLS certificate file.

**Default**: ``

### tlsTrustStorePassword
Password of TLS TrustStore.

**Default**: `null`

### tlsTrustStorePath
Path of TLS TrustStore.

**Default**: `null`

### tlsTrustStoreType
TLS TrustStore type configuration. You need to set this configuration when client authentication is required.

**Default**: `JKS`

### useKeyStoreTls
Set TLS using KeyStore way.

**Default**: `false`

### useTcpNoDelay
Whether to use TCP NoDelay option.

**Default**: `true`

### useTls
Whether to use TLS.

**Default**: `false`


