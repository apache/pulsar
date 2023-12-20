/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.proxy.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PropertiesContext;
import org.apache.pulsar.common.configuration.PropertyContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.sasl.SaslConstants;

@Getter
@Setter
public class ProxyConfiguration implements PulsarConfiguration {
    @Category
    private static final String CATEGORY_SERVER = "Server";
    @Category
    private static final String CATEGORY_BROKER_DISCOVERY = "Broker Discovery";
    @Category
    private static final String CATEGORY_BROKER_PROXY = "Broker Proxy";
    @Category
    private static final String CATEGORY_AUTHENTICATION = "Proxy Authentication";
    @Category
    private static final String CATEGORY_AUTHORIZATION = "Proxy Authorization";
    @Category(
        description = "the settings are for configuring how proxies authenticates with Pulsar brokers"
    )
    private static final String CATEGORY_CLIENT_AUTHENTICATION = "Broker Client Authorization";
    @Category
    private static final String CATEGORY_RATE_LIMITING = "RateLimiting";
    @Category
    private static final String CATEGORY_TLS = "TLS";
    @Category
    private static final String CATEGORY_KEYSTORE_TLS = "KeyStoreTLS";
    @Category
    private static final String CATEGORY_TOKEN_AUTH = "Token Authentication Provider";
    @Category
    private static final String CATEGORY_HTTP = "HTTP";
    @Category
    private static final String CATEGORY_SASL_AUTH = "SASL Authentication Provider";
    @Category
    private static final String CATEGORY_PLUGIN = "proxy plugin";
    @Category
    private static final String CATEGORY_WEBSOCKET = "WebSocket";

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        deprecated = true,
        doc = "The ZooKeeper quorum connection string (as a comma-separated list)"
    )
    @Deprecated
    private String zookeeperServers;

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            required = false,
            doc = "The metadata store URL. \n"
                    + " Examples: \n"
                    + "  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181\n"
                    + "  * my-zk-1:2181,my-zk-2:2181,my-zk-3:2181 (will default to ZooKeeper when the schema is not "
                    + "specified)\n"
                    + "  * zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181/my-chroot-path (to add a ZK chroot path)\n"
    )
    private String metadataStoreUrl;

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        deprecated = true,
        doc = "Configuration store connection string (as a comma-separated list)"
    )
    @Deprecated
    private String configurationStoreServers;

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        deprecated = true,
        doc = "Global ZooKeeper quorum connection string (as a comma-separated list)"
    )
    @Deprecated
    private String globalZookeeperServers;

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            required = false,
            doc = "The metadata store URL for the configuration data. If empty, we fall back to use metadataStoreUrl"
    )
    private String configurationMetadataStoreUrl;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Metadata store session timeout in milliseconds."
    )
    private int metadataStoreSessionTimeoutMillis = 30_000;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Metadata store cache expiry time in seconds."
    )
    private int metadataStoreCacheExpirySeconds = 300;

    @Deprecated
    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        deprecated = true,
        doc = "ZooKeeper session timeout in milliseconds. "
                + "@deprecated - Use metadataStoreSessionTimeoutMillis instead."
    )
    private int zookeeperSessionTimeoutMs = -1;

    @Deprecated
    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        deprecated = true,
        doc = "ZooKeeper cache expiry time in seconds. "
                + "@deprecated - Use metadataStoreCacheExpirySeconds instead."
    )
    private int zooKeeperCacheExpirySeconds = -1;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Is zooKeeper allow read-only operations."
    )
    private boolean zooKeeperAllowReadOnlyOperations;

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The service url points to the broker cluster"
    )
    private String brokerServiceURL;
    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The tls service url points to the broker cluster"
    )
    private String brokerServiceURLTLS;

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The web service url points to the broker cluster"
    )
    private String brokerWebServiceURL;
    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The tls web service url points to the broker cluster"
    )
    private String brokerWebServiceURLTLS;

    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The web service url points to the function worker cluster."
            + " Only configure it when you setup function workers in a separate cluster"
    )
    private String functionWorkerWebServiceURL;
    @FieldContext(
        category = CATEGORY_BROKER_DISCOVERY,
        doc = "The tls web service url points to the function worker cluster."
            + " Only configure it when you setup function workers in a separate cluster"
    )
    private String functionWorkerWebServiceURLTLS;

    @FieldContext(category = CATEGORY_BROKER_PROXY,
            doc = "When enabled, checks that the target broker is active before connecting. "
                    + "zookeeperServers and configurationStoreServers must be configured in proxy configuration "
                    + "for retrieving the active brokers.")
    private boolean checkActiveBrokers = false;

    @FieldContext(
            category = CATEGORY_BROKER_PROXY,
            doc = "Broker proxy connect timeout.\n"
                    + "The timeout value for Broker proxy connect timeout is in millisecond. Set to 0 to disable."
    )
    private int brokerProxyConnectTimeoutMs = 10000;

    @FieldContext(
            category = CATEGORY_BROKER_PROXY,
            doc = "Broker proxy read timeout.\n"
                    + "The timeout value for Broker proxy read timeout is in millisecond. Set to 0 to disable."
    )
    private int brokerProxyReadTimeoutMs = 75000;

    @FieldContext(
            category = CATEGORY_BROKER_PROXY,
            doc = "Allowed broker target host names. "
                    + "Supports multiple comma separated entries and a wildcard.")
    private String brokerProxyAllowedHostNames = "*";

    @FieldContext(
            category = CATEGORY_BROKER_PROXY,
            doc = "Allowed broker target ip addresses or ip networks / netmasks. "
                    + "Supports multiple comma separated entries.")
    private String brokerProxyAllowedIPAddresses = "*";

    @FieldContext(
            category = CATEGORY_BROKER_PROXY,
            doc = "Allowed broker target ports")
    private String brokerProxyAllowedTargetPorts = "6650,6651";

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Hostname or IP address the service binds on"
    )
    private String bindAddress = "0.0.0.0";

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Hostname or IP address the service advertises to the outside world."
            + " If not set, the value of `InetAddress.getLocalHost().getCanonicalHostName()` is used."
    )
    private String advertisedAddress;

    @FieldContext(category = CATEGORY_SERVER,
            doc = "Enable or disable the proxy protocol.")
    private boolean haProxyProtocolEnabled;

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving binary protobuf request"
    )
    private Optional<Integer> servicePort = Optional.ofNullable(6650);
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving tls secured binary protobuf request"
    )
    private Optional<Integer> servicePortTls = Optional.empty();

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving http requests"
    )
    private Optional<Integer> webServicePort = Optional.ofNullable(8080);
    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "The port for serving https requests"
    )
    private Optional<Integer> webServicePortTls = Optional.empty();

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Specify the TLS provider for the web service, available values can be SunJSSE, Conscrypt and etc."
    )
    private String webServiceTlsProvider = "Conscrypt";

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Specify the tls protocols the proxy's web service will use to negotiate during TLS Handshake.\n\n"
                    + "Example:- [TLSv1.3, TLSv1.2]"
    )
    private Set<String> webServiceTlsProtocols = new TreeSet<>();

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Specify the tls cipher the proxy's web service will use to negotiate during TLS Handshake.\n\n"
                    + "Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
    )
    private Set<String> webServiceTlsCiphers = new TreeSet<>();

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "The directory where nar Extraction happens"
    )
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Proxy log level, default is 0."
                    + " 0: Do not log any tcp channel info"
                    + " 1: Parse and log any tcp channel info and command info without message body"
                    + " 2: Parse and log channel info, command info and message body"
    )
    private Optional<Integer> proxyLogLevel = Optional.ofNullable(0);

    @FieldContext(
        category = CATEGORY_SERVER,
        doc = "Path for the file used to determine the rotation status for the proxy instance"
            + " when responding to service discovery health checks"
    )
    private String statusFilePath;

    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "A list of role names (a comma-separated list of strings) that are treated as"
            + " `super-user`, meaning they will be able to do all admin operations and publish"
            + " & consume from all topics"
    )
    private Set<String> superUserRoles = new TreeSet<>();

    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Whether authentication is enabled for the Pulsar proxy"
    )
    private boolean authenticationEnabled = false;
    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Authentication provider name list (a comma-separated list of class names"
    )
    private Set<String> authenticationProviders = new TreeSet<>();
    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Whether authorization is enforced by the Pulsar proxy"
    )
    private boolean authorizationEnabled = false;
    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Authorization provider as a fully qualified class name"
    )
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();
    @FieldContext(
        category = CATEGORY_AUTHORIZATION,
        doc = "Whether client authorization credentials are forwarded to the broker for re-authorization."
            + "Authentication must be enabled via configuring `authenticationEnabled` to be true for this"
            + "to take effect"
    )
    private boolean forwardAuthorizationCredentials = false;
    @FieldContext(
        category = CATEGORY_HTTP,
        doc = "Whether to enable the proxy's /metrics and /proxy-stats http endpoints"
    )
    private boolean enableProxyStatsEndpoints = true;

    @FieldContext(
        category = CATEGORY_AUTHENTICATION,
        doc = "Whether the '/metrics' endpoint requires authentication. Defaults to true."
            + "'authenticationEnabled' must also be set for this to take effect."
    )
    private boolean authenticateMetricsEndpoint = true;


    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL.\n"
            + " Default value is: \".*pulsar.*\", so only clients whose id contains 'pulsar' are allowed to connect."
    )
    private String saslJaasClientAllowedIds = SaslConstants.JAAS_CLIENT_ALLOWED_IDS_DEFAULT;

    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "Service Principal, for login context name. Default value is \"PulsarProxy\"."
    )
    private String saslJaasServerSectionName = SaslConstants.JAAS_DEFAULT_PROXY_SECTION_NAME;

    @FieldContext(
            category = CATEGORY_SASL_AUTH,
            doc = "Path to file containing the secret to be used to SaslRoleTokenSigner\n"
                    + "The secret can be specified like:\n"
                    + "saslJaasServerRoleTokenSignerSecretPath=file:///my/saslRoleTokenSignerSecret.key."
    )
    private String saslJaasServerRoleTokenSignerSecretPath;

    @FieldContext(
        category = CATEGORY_SASL_AUTH,
        doc = "kerberos kinit command."
    )
    private String kinitCommand = "/usr/bin/kinit";


    @FieldContext(
        category = CATEGORY_RATE_LIMITING,
        doc = "Max concurrent inbound connections. The proxy will reject requests beyond that"
    )
    private int maxConcurrentInboundConnections = 10000;

    @FieldContext(
        category = CATEGORY_RATE_LIMITING,
        doc = "Max concurrent lookup requests. The proxy will reject requests beyond that"
    )
    private int maxConcurrentLookupRequests = 50000;

    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientAuthenticationPlugin;
    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientAuthenticationParameters;
    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientTrustCertsFilePath;

    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "Whether TLS is enabled when communicating with Pulsar brokers"
    )
    private boolean tlsEnabledWithBroker = false;

    @FieldContext(
            category = CATEGORY_AUTHORIZATION,
            doc = "When this parameter is not empty, unauthenticated users perform as anonymousUserRole"
    )
    private String anonymousUserRole = null;

    // TLS

    @Deprecated
    private boolean tlsEnabledInProxy = false;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Tls cert refresh duration in seconds (set 0 to check on every new connection)"
    )
    private long tlsCertRefreshCheckDurationSec = 300; // 5 mins
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the TLS certificate file"
    )
    private String tlsCertificateFilePath;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the TLS private key file"
    )
    private String tlsKeyFilePath;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Path for the trusted TLS certificate file.\n\n"
            + "This cert is used to verify that any certs presented by connecting clients"
            + " are signed by a certificate authority. If this verification fails, then the"
            + " certs are untrusted and the connections are dropped"
    )
    private String tlsTrustCertsFilePath;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Accept untrusted TLS certificate from client.\n\n"
            + "If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath`"
            + " cert will be allowed to connect to the server, though the cert will not be used for"
            + " client authentication"
    )
    private boolean tlsAllowInsecureConnection = false;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Whether the hostname is validated when the proxy creates a TLS connection with brokers"
    )
    private boolean tlsHostnameVerificationEnabled = false;
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Specify the tls protocols the broker will use to negotiate during TLS handshake"
            + " (a comma-separated list of protocol names).\n\n"
            + "Examples:- [TLSv1.3, TLSv1.2]"
    )
    private Set<String> tlsProtocols = new TreeSet<>();
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Specify the tls cipher the proxy will use to negotiate during TLS Handshake"
            + " (a comma-separated list of ciphers).\n\n"
            + "Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
    )
    private Set<String> tlsCiphers = new TreeSet<>();
    @FieldContext(
        category = CATEGORY_TLS,
        doc = "Whether client certificates are required for TLS.\n\n"
            + " Connections are rejected if the client certificate isn't trusted"
    )
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    // KeyStore TLS config variables

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Enable TLS with KeyStore type configuration for proxy"
    )
    private boolean tlsEnabledWithKeyStore = false;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Specify the TLS provider for the broker service: \n"
                    + "When using TLS authentication with CACert, the valid value is either OPENSSL or JDK.\n"
                    + "When using TLS authentication with KeyStore, available values can be SunJSSE, Conscrypt and etc."
    )
    private String tlsProvider = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore type configuration for proxy: JKS, PKCS12"
    )
    private String tlsKeyStoreType = "JKS";

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore path for proxy"
    )
    private String tlsKeyStore = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore password for proxy"
    )
    private String tlsKeyStorePassword = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore type configuration for proxy: JKS, PKCS12"
    )
    private String tlsTrustStoreType = "JKS";

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore path for proxy"
    )
    private String tlsTrustStore = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore password for proxy, null means empty password."
    )
    private String tlsTrustStorePassword = null;

    /**
     * KeyStore TLS config variables used for proxy to auth with broker.
     */
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Whether the Pulsar proxy use KeyStore type to authenticate with Pulsar brokers"
    )
    private boolean brokerClientTlsEnabledWithKeyStore = false;
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "The TLS Provider used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientSslProvider = null;

    // needed when client auth is required
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore type configuration for proxy: JKS, PKCS12 "
                  + " used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStoreType = "JKS";
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore path for proxy, "
                  + " used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStore = null;
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore password for proxy, "
                  + " used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStorePassword = null;
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Specify the tls cipher the proxy will use to negotiate during TLS Handshake"
                  + " (a comma-separated list of ciphers).\n\n"
                  + "Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].\n"
                  + " used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private Set<String> brokerClientTlsCiphers = new TreeSet<>();
    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Specify the tls protocols the broker will use to negotiate during TLS handshake"
                  + " (a comma-separated list of protocol names).\n\n"
                  + "Examples:- [TLSv1.3, TLSv1.2] \n"
                  + " used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private Set<String> brokerClientTlsProtocols = new TreeSet<>();

    // HTTP

    @FieldContext(
        category = CATEGORY_HTTP,
        doc = "Http directs to redirect to non-pulsar services"
    )
    private Set<HttpReverseProxyConfig> httpReverseProxyConfigs = new HashSet<>();

    @FieldContext(
        minValue = 1,
        category = CATEGORY_HTTP,
        doc = "Http output buffer size.\n\n"
            + "The amount of data that will be buffered for http requests "
            + "before it is flushed to the channel. A larger buffer size may "
            + "result in higher http throughput though it may take longer for "
            + "the client to see data. If using HTTP streaming via the reverse "
            + "proxy, this should be set to the minimum value, 1, so that clients "
            + "see the data as soon as possible."
    )
    private int httpOutputBufferSize = 32 * 1024;

    @FieldContext(
            minValue = 1,
            category = CATEGORY_HTTP,
            doc = "Http input buffer max size.\n\n"
                    + "The maximum amount of data that will be buffered for incoming http requests "
                    + "so that the request body can be replayed when the backend broker "
                    + "issues a redirect response."
    )
    private int httpInputMaxReplayBufferSize = 5 * 1024 * 1024;

    @FieldContext(
            minValue = 1,
            category = CATEGORY_HTTP,
            doc = "Http proxy timeout.\n\n"
                    + "The timeout value for HTTP proxy is in millisecond."
    )
    private int httpProxyTimeout = 5 * 60 * 1000;

    @FieldContext(
           minValue = 1,
           category = CATEGORY_HTTP,
           doc = "Number of threads to use for HTTP requests processing"
    )
    private int httpNumThreads = Math.max(8, 2 * Runtime.getRuntime().availableProcessors());

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Number of threads used for Netty IO."
                    + " Default is set to `2 * Runtime.getRuntime().availableProcessors()`"
    )
    private int numIOThreads = 2 * Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "Number of threads used for Netty Acceptor."
                    + " Default is set to `1`"
    )
    private int numAcceptorThreads = 1;

    @Deprecated
    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "The directory to locate proxy additional servlet"
    )
    private String proxyAdditionalServletDirectory = "./proxyAdditionalServlet";

    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "The directory to locate proxy additional servlet"
    )
    private String additionalServletDirectory = "./proxyAdditionalServlet";

    @Deprecated
    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "List of proxy additional servlet to load, which is a list of proxy additional servlet names"
    )
    private Set<String> proxyAdditionalServlets = new TreeSet<>();

    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "List of proxy additional servlet to load, which is a list of proxy additional servlet names"
    )
    private Set<String> additionalServlets = new TreeSet<>();

    @FieldContext(
            category = CATEGORY_HTTP,
            doc = "Enable the enforcement of limits on the incoming HTTP requests"
    )
    private boolean httpRequestsLimitEnabled = false;

    @FieldContext(
            category = CATEGORY_HTTP,
            doc = "Max HTTP requests per seconds allowed."
                    + " The excess of requests will be rejected with HTTP code 429 (Too many requests)"
    )
    private double httpRequestsMaxPerSecond = 100.0;

    @PropertiesContext(
        properties = {
            @PropertyContext(
                key = "tokenPublicKey",
                doc = @FieldContext(
                    category = CATEGORY_TOKEN_AUTH,
                    doc = "Asymmetric public/private key pair.\n\n"
                        + "Configure the public key to be used to validate auth tokens"
                        + " The key can be specified like:\n\n"
                        + "tokenPublicKey=data:;base64,xxxxxxxxx\n"
                        + "tokenPublicKey=file:///my/public.key  ( Note: key file must be DER-encoded )")
            ),
            @PropertyContext(
                key = "tokenSecretKey",
                doc = @FieldContext(
                    category = CATEGORY_TOKEN_AUTH,
                    doc = "Symmetric key.\n\n"
                        + "Configure the secret key to be used to validate auth tokens"
                        + "The key can be specified like:\n\n"
                        + "tokenSecretKey=data:;base64,xxxxxxxxx\n"
                        + "tokenSecretKey=file:///my/secret.key  ( Note: key file must be DER-encoded )")
            )
        }
    )

    /***** --- Protocol Handlers --- ****/
    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "The directory to locate proxy extensions"
    )
    private String proxyExtensionsDirectory = "./proxyextensions";

    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "List of messaging protocols to load, which is a list of extension names"
    )
    private Set<String> proxyExtensions = new TreeSet<>();

    @FieldContext(
            category = CATEGORY_PLUGIN,
            doc = "Use a separate ThreadPool for each Proxy Extension"
    )
    private boolean useSeparateThreadPoolForProxyExtensions = true;

    /***** --- WebSocket. --- ****/
    @FieldContext(
            category = CATEGORY_WEBSOCKET,
            doc = "Enable or disable the WebSocket servlet"
    )
    private boolean webSocketServiceEnabled = false;

    @FieldContext(
            category = CATEGORY_WEBSOCKET,
            doc = "Interval of time to sending the ping to keep alive in WebSocket proxy. "
                    + "This value greater than 0 means enabled")
    private int webSocketPingDurationSeconds = -1;

    @FieldContext(
            category = CATEGORY_WEBSOCKET,
            doc = "Name of the cluster to which this broker belongs to"
    )
    private String clusterName;

    public String getMetadataStoreUrl() {
        if (StringUtils.isNotBlank(metadataStoreUrl)) {
            return metadataStoreUrl;
        } else {
            // Fallback to old setting
            return zookeeperServers;
        }
    }

    public String getConfigurationMetadataStoreUrl() {
        if (StringUtils.isNotBlank(configurationMetadataStoreUrl)) {
            return configurationMetadataStoreUrl;
        } else if (StringUtils.isNotBlank(configurationStoreServers)) {
            return configurationStoreServers;
        } else if (StringUtils.isNotBlank(globalZookeeperServers)) {
            return globalZookeeperServers;
        } else {
            // Fallback to local zookeeper
            return getMetadataStoreUrl();
        }
    }

    private Properties properties = new Properties();

    public Properties getProperties() {
        return properties;
    }

    public Optional<Integer> getServicePort() {
        return servicePort;
    }

    public Optional<Integer> getServicePortTls() {
        return servicePortTls;
    }

    public Optional<Integer> getWebServicePort() {
        return webServicePort;
    }

    public Optional<Integer> getWebServicePortTls() {
        return webServicePortTls;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;

        Map<String, Map<String, String>> redirects = new HashMap<>();
        Pattern redirectPattern = Pattern.compile("^httpReverseProxy\\.([^\\.]*)\\.(.+)$");
        Map<String, List<Matcher>> groups = properties.stringPropertyNames().stream()
            .map((s) -> redirectPattern.matcher(s))
            .filter(Matcher::matches)
            .collect(Collectors.groupingBy((m) -> m.group(1))); // group by name

        groups.entrySet().forEach((e) -> {
                Map<String, String> keyToFullKey = e.getValue().stream().collect(
                        Collectors.toMap(m -> m.group(2), m -> m.group(0)));
                if (!keyToFullKey.containsKey("path")) {
                    throw new IllegalArgumentException(
                            String.format("httpReverseProxy.%s.path must be specified exactly once", e.getKey()));
                }
                if (!keyToFullKey.containsKey("proxyTo")) {
                    throw new IllegalArgumentException(
                            String.format("httpReverseProxy.%s.proxyTo must be specified exactly once", e.getKey()));
                }
                httpReverseProxyConfigs.add(new HttpReverseProxyConfig(e.getKey(),
                                                    properties.getProperty(keyToFullKey.get("path")),
                                                    properties.getProperty(keyToFullKey.get("proxyTo"))));
            });
    }

    public static class HttpReverseProxyConfig {
        private final String name;
        private final String path;
        private final String proxyTo;

        HttpReverseProxyConfig(String name, String path, String proxyTo) {
            this.name = name;
            this.path = path;
            this.proxyTo = proxyTo;
        }

        public String getName() {
            return name;
        }

        public String getPath() {
            return path;
        }

        public String getProxyTo() {
            return proxyTo;
        }

        @Override
        public String toString() {
            return String.format("HttpReverseProxyConfig(%s, path=%s, proxyTo=%s)", name, path, proxyTo);
        }
    }

    public int getMetadataStoreSessionTimeoutMillis() {
        return zookeeperSessionTimeoutMs > 0 ? zookeeperSessionTimeoutMs : metadataStoreSessionTimeoutMillis;
    }

    public int getMetadataStoreCacheExpirySeconds() {
        return zooKeeperCacheExpirySeconds > 0 ? zooKeeperCacheExpirySeconds : metadataStoreCacheExpirySeconds;
    }
}
