# Pulsar proxy
:::note

This page is automatically generated from code files.
If you find something inaccurate, feel free to update `org.apache.pulsar.proxy.server.ProxyConfiguration`. Do NOT edit this markdown file manually. Manual changes will be overwritten by automatic generation.

:::
## Required
## Optional
### brokerClientAuthenticationParameters
The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Client Authorization

### brokerClientAuthenticationPlugin
The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Client Authorization

### brokerClientTrustCertsFilePath
The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Client Authorization

### tlsEnabledWithBroker
Whether TLS is enabled when communicating with Pulsar brokers

**Default**: `false`

**Dynamic**: `false`

**Category**: Broker Client Authorization

### brokerServiceURL
The service url points to the broker cluster. URL must have the pulsar:// prefix.

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### brokerServiceURLTLS
The tls service url points to the broker cluster. URL must have the pulsar+ssl:// prefix.

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### brokerWebServiceURL
The web service url points to the broker cluster

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### brokerWebServiceURLTLS
The tls web service url points to the broker cluster

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### configurationMetadataStoreUrl
The metadata store URL for the configuration data. If empty, we fall back to use metadataStoreUrl

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### functionWorkerWebServiceURL
The web service url points to the function worker cluster. Only configure it when you setup function workers in a separate cluster

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### functionWorkerWebServiceURLTLS
The tls web service url points to the function worker cluster. Only configure it when you setup function workers in a separate cluster

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### metadataStoreUrl
The metadata store URL. 
 Examples: 
  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181
  * my-zk-1:2181,my-zk-2:2181,my-zk-3:2181 (will default to ZooKeeper when the schema is not specified)
  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181/my-chroot-path (to add a ZK chroot path)


**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### brokerProxyAllowedHostNames
Allowed broker target host names. Supports multiple comma separated entries and a wildcard.

**Default**: `*`

**Dynamic**: `false`

**Category**: Broker Proxy

### brokerProxyAllowedIPAddresses
Allowed broker target ip addresses or ip networks / netmasks. Supports multiple comma separated entries.

**Default**: `*`

**Dynamic**: `false`

**Category**: Broker Proxy

### brokerProxyAllowedTargetPorts
Allowed broker target ports

**Default**: `6650,6651`

**Dynamic**: `false`

**Category**: Broker Proxy

### brokerProxyConnectTimeoutMs
Broker proxy connect timeout.
The timeout value for Broker proxy connect timeout is in millisecond. Set to 0 to disable.

**Default**: `10000`

**Dynamic**: `false`

**Category**: Broker Proxy

### brokerProxyReadTimeoutMs
Broker proxy read timeout.
The timeout value for Broker proxy read timeout is in millisecond. Set to 0 to disable.

**Default**: `75000`

**Dynamic**: `false`

**Category**: Broker Proxy

### checkActiveBrokers
When enabled, checks that the target broker is active before connecting. zookeeperServers and configurationStoreServers must be configured in proxy configuration for retrieving the active brokers.

**Default**: `false`

**Dynamic**: `false`

**Category**: Broker Proxy

### httpInputMaxReplayBufferSize
Http input buffer max size.

The maximum amount of data that will be buffered for incoming http requests so that the request body can be replayed when the backend broker issues a redirect response.

**Default**: `5242880`

**Dynamic**: `false`

**Category**: HTTP

### httpNumThreads
Number of threads to use for HTTP requests processing

**Default**: `16`

**Dynamic**: `false`

**Category**: HTTP

### httpOutputBufferSize
Http output buffer size.

The amount of data that will be buffered for http requests before it is flushed to the channel. A larger buffer size may result in higher http throughput though it may take longer for the client to see data. If using HTTP streaming via the reverse proxy, this should be set to the minimum value, 1, so that clients see the data as soon as possible.

**Default**: `32768`

**Dynamic**: `false`

**Category**: HTTP

### httpProxyTimeout
Http proxy timeout.

The timeout value for HTTP proxy is in millisecond.

**Default**: `300000`

**Dynamic**: `false`

**Category**: HTTP

### httpRequestsLimitEnabled
Enable the enforcement of limits on the incoming HTTP requests

**Default**: `false`

**Dynamic**: `false`

**Category**: HTTP

### httpRequestsMaxPerSecond
Max HTTP requests per seconds allowed. The excess of requests will be rejected with HTTP code 429 (Too many requests)

**Default**: `100.0`

**Dynamic**: `false`

**Category**: HTTP

### httpReverseProxyConfigs
Http directs to redirect to non-pulsar services

**Default**: `[]`

**Dynamic**: `false`

**Category**: HTTP

### brokerClientSslProvider
The TLS Provider used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsCiphers
Specify the tls cipher the proxy will use to negotiate during TLS Handshake (a comma-separated list of ciphers).

Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].
 used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `[]`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsEnabledWithKeyStore
Whether the Pulsar proxy use KeyStore type to authenticate with Pulsar brokers

**Default**: `false`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsProtocols
Specify the tls protocols the broker will use to negotiate during TLS handshake (a comma-separated list of protocol names).

Examples:- [TLSv1.3, TLSv1.2] 
 used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `[]`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsTrustStore
TLS TrustStore path for proxy,  used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsTrustStorePassword
TLS TrustStore password for proxy,  used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### brokerClientTlsTrustStoreType
TLS TrustStore type configuration for proxy: JKS, PKCS12  used by the Pulsar proxy to authenticate with Pulsar brokers

**Default**: `JKS`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsEnabledWithKeyStore
Enable TLS with KeyStore type configuration for proxy

**Default**: `false`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsKeyStore
TLS KeyStore path for proxy

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsKeyStorePassword
TLS KeyStore password for proxy

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsKeyStoreType
TLS KeyStore type configuration for proxy: JKS, PKCS12

**Default**: `JKS`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsProvider
Specify the TLS provider for the broker service: 
When using TLS authentication with CACert, the valid value is either OPENSSL or JDK.
When using TLS authentication with KeyStore, available values can be SunJSSE, Conscrypt and etc.

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsTrustStore
TLS TrustStore path for proxy

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsTrustStorePassword
TLS TrustStore password for proxy, null means empty password.

**Default**: `null`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### tlsTrustStoreType
TLS TrustStore type configuration for proxy: JKS, PKCS12

**Default**: `JKS`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### webServiceTlsProvider
Specify the TLS provider for the web service, available values can be SunJSSE, Conscrypt and etc.

**Default**: `Conscrypt`

**Dynamic**: `false`

**Category**: KeyStoreTLS

### authenticateMetricsEndpoint
Whether the '/metrics' endpoint requires authentication. Defaults to true.'authenticationEnabled' must also be set for this to take effect.

**Default**: `true`

**Dynamic**: `false`

**Category**: Proxy Authentication

### authenticationEnabled
Whether authentication is enabled for the Pulsar proxy

**Default**: `false`

**Dynamic**: `false`

**Category**: Proxy Authentication

### authenticationProviders
Authentication provider name list (a comma-separated list of class names

**Default**: `[]`

**Dynamic**: `false`

**Category**: Proxy Authentication

### anonymousUserRole
When this parameter is not empty, unauthenticated users perform as anonymousUserRole

**Default**: `null`

**Dynamic**: `false`

**Category**: Proxy Authorization

### authorizationEnabled
Whether authorization is enforced by the Pulsar proxy

**Default**: `false`

**Dynamic**: `false`

**Category**: Proxy Authorization

### authorizationProvider
Authorization provider as a fully qualified class name

**Default**: `org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider`

**Dynamic**: `false`

**Category**: Proxy Authorization

### forwardAuthorizationCredentials
Whether client authorization credentials are forwarded to the broker for re-authorization.Authentication must be enabled via configuring `authenticationEnabled` to be true for thisto take effect

**Default**: `false`

**Dynamic**: `false`

**Category**: Proxy Authorization

### superUserRoles
A list of role names (a comma-separated list of strings) that are treated as `super-user`, meaning they will be able to do all admin operations and publish & consume from all topics

**Default**: `[]`

**Dynamic**: `false`

**Category**: Proxy Authorization

### maxConcurrentInboundConnections
Max concurrent inbound connections. The proxy will reject requests beyond that

**Default**: `10000`

**Dynamic**: `false`

**Category**: RateLimiting

### maxConcurrentLookupRequests
Max concurrent lookup requests. The proxy will reject requests beyond that

**Default**: `50000`

**Dynamic**: `false`

**Category**: RateLimiting

### kinitCommand
kerberos kinit command.

**Default**: `/usr/bin/kinit`

**Dynamic**: `false`

**Category**: SASL Authentication Provider

### saslJaasClientAllowedIds
This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL.
 Default value is: ".*pulsar.*", so only clients whose id contains 'pulsar' are allowed to connect.

**Default**: `.*pulsar.*`

**Dynamic**: `false`

**Category**: SASL Authentication Provider

### saslJaasServerRoleTokenSignerSecretPath
Path to file containing the secret to be used to SaslRoleTokenSigner
The secret can be specified like:
saslJaasServerRoleTokenSignerSecretPath=file:///my/saslRoleTokenSignerSecret.key.

**Default**: `null`

**Dynamic**: `false`

**Category**: SASL Authentication Provider

### saslJaasServerSectionName
Service Principal, for login context name. Default value is "PulsarProxy".

**Default**: `PulsarProxy`

**Dynamic**: `false`

**Category**: SASL Authentication Provider

### advertisedAddress
Hostname or IP address the service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getCanonicalHostName()` is used.

**Default**: `null`

**Dynamic**: `false`

**Category**: Server

### bindAddress
Hostname or IP address the service binds on

**Default**: `0.0.0.0`

**Dynamic**: `false`

**Category**: Server

### haProxyProtocolEnabled
Enable or disable the proxy protocol.

**Default**: `false`

**Dynamic**: `false`

**Category**: Server

### httpServerAcceptQueueSize
Capacity for accept queue in the HTTP server Default is set to 8192.

**Default**: `8192`

**Dynamic**: `false`

**Category**: Server

### httpServerThreadPoolQueueSize
Capacity for thread pool queue in the HTTP server Default is set to 8192.

**Default**: `8192`

**Dynamic**: `false`

**Category**: Server

### maxConcurrentHttpRequests
Max concurrent web requests

**Default**: `1024`

**Dynamic**: `false`

**Category**: Server

### maxHttpServerConnections
Maximum number of inbound http connections. (0 to disable limiting)

**Default**: `2048`

**Dynamic**: `false`

**Category**: Server

### metadataStoreCacheExpirySeconds
Metadata store cache expiry time in seconds.

**Default**: `300`

**Dynamic**: `false`

**Category**: Server

### metadataStoreSessionTimeoutMillis
Metadata store session timeout in milliseconds.

**Default**: `30000`

**Dynamic**: `false`

**Category**: Server

### narExtractionDirectory
The directory where nar Extraction happens

**Default**: `/var/folders/0y/136crjnx0sb33_71mj2b33nh0000gn/T/`

**Dynamic**: `false`

**Category**: Server

### numAcceptorThreads
Number of threads used for Netty Acceptor. Default is set to `1`

**Default**: `1`

**Dynamic**: `false`

**Category**: Server

### numIOThreads
Number of threads used for Netty IO. Default is set to `2 * Runtime.getRuntime().availableProcessors()`

**Default**: `16`

**Dynamic**: `false`

**Category**: Server

### proxyLogLevel
Proxy log level, default is 0. 0: Do not log any tcp channel info 1: Parse and log any tcp channel info and command info without message body 2: Parse and log channel info, command info and message body

**Default**: `Optional[0]`

**Dynamic**: `false`

**Category**: Server

### proxyZeroCopyModeEnabled
Enables zero-copy transport of data across network interfaces using the spice. Zero copy mode cannot be used when TLS is enabled or when proxyLogLevel is \> 0.

**Default**: `true`

**Dynamic**: `false`

**Category**: Server

### servicePort
The port for serving binary protobuf request

**Default**: `Optional[6650]`

**Dynamic**: `false`

**Category**: Server

### servicePortTls
The port for serving tls secured binary protobuf request

**Default**: `Optional.empty`

**Dynamic**: `false`

**Category**: Server

### statusFilePath
Path for the file used to determine the rotation status for the proxy instance when responding to service discovery health checks

**Default**: `null`

**Dynamic**: `false`

**Category**: Server

### webServicePort
The port for serving http requests

**Default**: `Optional[8080]`

**Dynamic**: `false`

**Category**: Server

### webServicePortTls
The port for serving https requests

**Default**: `Optional.empty`

**Dynamic**: `false`

**Category**: Server

### tlsAllowInsecureConnection
Accept untrusted TLS certificate from client.

If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath` cert will be allowed to connect to the server, though the cert will not be used for client authentication

**Default**: `false`

**Dynamic**: `false`

**Category**: TLS

### tlsCertRefreshCheckDurationSec
Tls cert refresh duration in seconds (set 0 to check on every new connection)

**Default**: `300`

**Dynamic**: `false`

**Category**: TLS

### tlsCertificateFilePath
Path for the TLS certificate file

**Default**: `null`

**Dynamic**: `false`

**Category**: TLS

### tlsCiphers
Specify the tls cipher the proxy will use to negotiate during TLS Handshake (a comma-separated list of ciphers).

Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]

**Default**: `[]`

**Dynamic**: `false`

**Category**: TLS

### tlsHostnameVerificationEnabled
Whether the hostname is validated when the proxy creates a TLS connection with brokers

**Default**: `false`

**Dynamic**: `false`

**Category**: TLS

### tlsKeyFilePath
Path for the TLS private key file

**Default**: `null`

**Dynamic**: `false`

**Category**: TLS

### tlsProtocols
Specify the tls protocols the broker will use to negotiate during TLS handshake (a comma-separated list of protocol names).

Examples:- [TLSv1.3, TLSv1.2]

**Default**: `[]`

**Dynamic**: `false`

**Category**: TLS

### tlsRequireTrustedClientCertOnConnect
Whether client certificates are required for TLS.

 Connections are rejected if the client certificate isn't trusted

**Default**: `false`

**Dynamic**: `false`

**Category**: TLS

### tlsTrustCertsFilePath
Path for the trusted TLS certificate file.

This cert is used to verify that any certs presented by connecting clients are signed by a certificate authority. If this verification fails, then the certs are untrusted and the connections are dropped

**Default**: `null`

**Dynamic**: `false`

**Category**: TLS

### webServiceTlsCiphers
Specify the tls cipher the proxy's web service will use to negotiate during TLS Handshake.

Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]

**Default**: `[]`

**Dynamic**: `false`

**Category**: TLS

### webServiceTlsProtocols
Specify the tls protocols the proxy's web service will use to negotiate during TLS Handshake.

Example:- [TLSv1.3, TLSv1.2]

**Default**: `[]`

**Dynamic**: `false`

**Category**: TLS

### clusterName
Name of the cluster to which this broker belongs to

**Default**: `null`

**Dynamic**: `false`

**Category**: WebSocket

### webSocketServiceEnabled
Enable or disable the WebSocket servlet

**Default**: `false`

**Dynamic**: `false`

**Category**: WebSocket

### additionalServletDirectory
The directory to locate proxy additional servlet

**Default**: `./proxyAdditionalServlet`

**Dynamic**: `false`

**Category**: proxy plugin

### additionalServlets
List of proxy additional servlet to load, which is a list of proxy additional servlet names

**Default**: `[]`

**Dynamic**: `false`

**Category**: proxy plugin

### proxyAdditionalServletDirectory
The directory to locate proxy additional servlet

**Default**: `./proxyAdditionalServlet`

**Dynamic**: `false`

**Category**: proxy plugin

### proxyAdditionalServlets
List of proxy additional servlet to load, which is a list of proxy additional servlet names

**Default**: `[]`

**Dynamic**: `false`

**Category**: proxy plugin

### proxyExtensions
List of messaging protocols to load, which is a list of extension names

**Default**: `[]`

**Dynamic**: `false`

**Category**: proxy plugin

### proxyExtensionsDirectory
The directory to locate proxy extensions

**Default**: `./proxyextensions`

**Dynamic**: `false`

**Category**: proxy plugin

### useSeparateThreadPoolForProxyExtensions
Use a separate ThreadPool for each Proxy Extension

**Default**: `true`

**Dynamic**: `false`

**Category**: proxy plugin

## Deprecated
### configurationStoreServers
Configuration store connection string (as a comma-separated list). Deprecated in favor of `configurationMetadataStoreUrl`

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### globalZookeeperServers
Global ZooKeeper quorum connection string (as a comma-separated list)

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### zooKeeperCacheExpirySeconds
ZooKeeper cache expiry time in seconds. @deprecated - Use metadataStoreCacheExpirySeconds instead.

**Default**: `-1`

**Dynamic**: `false`

**Category**: Broker Discovery

### zookeeperServers
The ZooKeeper quorum connection string (as a comma-separated list)

**Default**: `null`

**Dynamic**: `false`

**Category**: Broker Discovery

### zookeeperSessionTimeoutMs
ZooKeeper session timeout in milliseconds. @deprecated - Use metadataStoreSessionTimeoutMillis instead.

**Default**: `-1`

**Dynamic**: `false`

**Category**: Broker Discovery


