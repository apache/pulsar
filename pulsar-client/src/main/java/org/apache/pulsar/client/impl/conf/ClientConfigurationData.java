/*
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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.OpenTelemetry;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.util.Secret;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;


/**
 * This is a simple holder of the client configuration values.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    @Schema(
            name = "serviceUrl",
            requiredMode = Schema.RequiredMode.REQUIRED,
            description = "Pulsar cluster HTTP URL to connect to a broker."
    )
    private String serviceUrl;
    @Schema(
            name = "serviceUrlProvider",
            description = "The implementation class of ServiceUrlProvider used to generate ServiceUrl."
    )
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    @Schema(
            name = "serviceUrlQuarantineInitDurationMs",
            description = "The initial duration (in milliseconds) to quarantine endpoints that fail to connect."
                    + " A value of 0 means don't quarantine any endpoints even if they fail."
    )
    private long serviceUrlQuarantineInitDurationMs = 60000;

    @Schema(
            name = "serviceUrlQuarantineMaxDurationMs",
            description = "The max duration (in milliseconds) to quarantine endpoints that fail to connect."
                    + " A value of 0 means don't quarantine any endpoints even if they fail."
    )
    private long serviceUrlQuarantineMaxDurationMs = TimeUnit.DAYS.toMillis(1);

    // Internal: set by the v5 SDK (PulsarClientBuilderV5), not exposed on the public ClientBuilder.
    // When true, transactions use the metadata-driven (PIP-473) coordinator; when false (v4 SDK),
    // they use the legacy coordinator. Routes coexistence at the TC layer by client/SDK kind rather
    // than broker capability, so a v4 client keeps using the legacy TC even on a v5-enabled cluster.
    @JsonIgnore
    private boolean scalableTransactions = false;

    @Schema(
            name = "authentication",
            description = "Authentication settings of the client."
    )
    @JsonIgnore
    private Authentication authentication;

    @Schema(
            name = "authPluginClassName",
            description = "Class name of authentication plugin of the client."
    )
    private String authPluginClassName;

    @Schema(
            name = "authParams",
            description = "Authentication parameter of the client."
    )
    @Secret
    private String authParams;

    @Schema(
            name = "authParamMap",
            description = "Authentication map of the client."
    )
    @Secret
    private Map<String, String> authParamMap;

    @Schema(
            name = "originalPrincipal",
            description = "Original principal for proxy authentication scenarios."
    )
    private String originalPrincipal;

    @Schema(
            name = "operationTimeoutMs",
            description = "Client operation timeout (in milliseconds)."
    )
    private long operationTimeoutMs = 30000;

    @Schema(
            name = "lookupTimeoutMs",
            description = "Client lookup timeout (in milliseconds)."
    )
    private long lookupTimeoutMs = -1;

    @Schema(
            name = "statsIntervalSeconds",
            description = "Interval to print client stats (in seconds)."
    )
    private long statsIntervalSeconds = 60;

    @Schema(
            name = "numIoThreads",
            description = "Number of IO threads."
    )
    private int numIoThreads = Runtime.getRuntime().availableProcessors();

    @Schema(
            name = "numListenerThreads",
            description = "Number of consumer listener threads."
    )
    private int numListenerThreads = Runtime.getRuntime().availableProcessors();

    @Schema(
            name = "connectionsPerBroker",
            description = "Number of connections established between the client and each Broker."
                    + " A value of 0 means to disable connection pooling."
    )
    private int connectionsPerBroker = 1;

    @Schema(
            name = "connectionMaxIdleSeconds",
            description = "Release the connection if it is not used for more than [connectionMaxIdleSeconds] seconds. "
                    + "If [connectionMaxIdleSeconds] < 0, disables the feature that auto-releases the idle connections"
    )
    private int connectionMaxIdleSeconds = 60;

    @Schema(
            name = "useTcpNoDelay",
            description = "Whether to use TCP NoDelay option."
    )
    private boolean useTcpNoDelay = true;

    @Schema(
            name = "useTls",
            description = "Whether to use TLS."
    )
    private boolean useTls = false;

    @Schema(
            name = "tlsKeyFilePath",
            description = "Path to the TLS key file."
    )
    private String tlsKeyFilePath = null;

    @Schema(
            name = "tlsCertificateFilePath",
            description = "Path to the TLS certificate file."
    )
    private String tlsCertificateFilePath = null;

    @Schema(
            name = "tlsTrustCertsFilePath",
            description = "Path to the trusted TLS certificate file."
    )
    private String tlsTrustCertsFilePath = null;

    @Schema(
            name = "tlsAllowInsecureConnection",
            description = "Whether the client accepts untrusted TLS certificates from the broker."
    )
    private boolean tlsAllowInsecureConnection = false;

    @Schema(
            name = "tlsHostnameVerificationEnable",
            description = "Whether the hostname is validated when the client creates a TLS connection with brokers."
    )
    private boolean tlsHostnameVerificationEnable = false;

    @Schema(
            name = "sslFactoryPlugin",
            description = "SSL Factory Plugin class to provide SSLEngine and SSLContext objects. The default "
                    + "class used is DefaultPulsarSslFactory.")
    private String sslFactoryPlugin = DefaultPulsarSslFactory.class.getName();

    @Schema(
            name = "sslFactoryPluginParams",
            description = "SSL Factory plugin configuration parameters.")
    private String sslFactoryPluginParams = "";

    @Schema(
            name = "concurrentLookupRequest",
            description = "The number of concurrent lookup requests that can be sent on each broker connection. "
                    + "Setting a maximum prevents overloading a broker."
    )
    private int concurrentLookupRequest = 5000;

    @Schema(
            name = "maxLookupRequest",
            description = "Maximum number of lookup requests allowed on "
                    + "each broker connection to prevent overloading a broker."
    )
    private int maxLookupRequest = 50000;

    @Schema(
            name = "maxLookupRedirects",
            description = "Maximum times of redirected lookup requests."
    )
    private int maxLookupRedirects = 20;

    @Schema(
            name = "maxNumberOfRejectedRequestPerConnection",
            description = "Maximum number of rejected requests of a broker in a certain time frame (60 seconds) "
                    + "after the current connection is closed and the client "
                    + "creating a new connection to connect to a different broker."
    )
    private int maxNumberOfRejectedRequestPerConnection = 50;

    @Schema(
            name = "keepAliveIntervalSeconds",
            description = "Seconds of keeping alive interval for each client broker connection."
    )
    private int keepAliveIntervalSeconds = 30;

    @Schema(
            name = "connectionTimeoutMs",
            description = "Duration of waiting for a connection to a broker to be established."
                    + " If the duration passes without a response from a broker, the connection attempt is dropped."
    )
    private int connectionTimeoutMs = 10000;
    @Schema(
            name = "requestTimeoutMs",
            description = "Maximum duration for completing a request."
    )
    private int requestTimeoutMs = 60000;

    @Schema(
            name = "readTimeoutMs",
            description = "Maximum read time of a request."
    )
    private int readTimeoutMs = 60000;

    @Schema(
            name = "autoCertRefreshSeconds",
            description = "Seconds of auto refreshing certificate."
    )
    private int autoCertRefreshSeconds = 300;

    @Schema(
            name = "initialBackoffIntervalNanos",
            description = "Initial backoff interval (in nanosecond)."
    )
    private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

    @Schema(
            name = "maxBackoffIntervalNanos",
            description = "Max backoff interval (in nanosecond)."
    )
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(60);

    @Schema(
            name = "enableBusyWait",
            description = "Whether to enable BusyWait for EpollEventLoopGroup."
    )
    private boolean enableBusyWait = false;

    @Schema(
            name = "listenerName",
            description = "Listener name for lookup. Clients can use listenerName to choose one of the listeners "
                    + "as the service URL to create a connection to the broker as long as the network is accessible."
                    + " \"advertisedListeners\" must be enabled on the broker side."
    )
    private String listenerName;

    @Schema(
            name = "useKeyStoreTls",
            description = "Set TLS using KeyStore way."
    )
    private boolean useKeyStoreTls = false;
    @Schema(
            name = "sslProvider",
            description = "The TLS provider used by an internal client to authenticate with other Pulsar brokers."
    )
    private String sslProvider = null;

    @Schema(
            name = "tlsKeyStoreType",
            description = "TLS KeyStore type configuration."
    )
    private String tlsKeyStoreType = "JKS";

    @Schema(
            name = "tlsKeyStorePath",
            description = "Path of TLS KeyStore."
    )
    private String tlsKeyStorePath = null;

    @Schema(
            name = "tlsKeyStorePassword",
            description = "Password of TLS KeyStore."
    )
    @Secret
    private String tlsKeyStorePassword = null;

    @Schema(
            name = "tlsTrustStoreType",
            description = "TLS TrustStore type configuration. You need to set this configuration when client "
                    + "authentication is required."
    )
    private String tlsTrustStoreType = "JKS";

    @Schema(
            name = "tlsTrustStorePath",
            description = "Path of TLS TrustStore."
    )
    private String tlsTrustStorePath = null;

    @Schema(
            name = "tlsTrustStorePassword",
            description = "Password of TLS TrustStore."
    )
    @Secret
    private String tlsTrustStorePassword = null;

    @Schema(
            name = "tlsCiphers",
            description = "Set of TLS Ciphers."
    )
    private Set<String> tlsCiphers = new TreeSet<>();

    @Schema(
            name = "tlsProtocols",
            description = "Protocols of TLS."
    )
    private Set<String> tlsProtocols = new TreeSet<>();

    @Schema(
            name = "memoryLimitBytes",
            description = "Limit of client memory usage (in byte). The 64M default can guarantee a high producer "
                    + "throughput."
    )
    private long memoryLimitBytes = 64 * 1024 * 1024;

    @Schema(
            name = "proxyServiceUrl",
            description = "URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private String proxyServiceUrl;

    @Schema(
            name = "proxyProtocol",
            description = "Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private ProxyProtocol proxyProtocol;

    @Schema(
            name = "enableTransaction",
            description = "Whether to enable transaction."
    )
    private boolean enableTransaction = false;

    @JsonIgnore
    private Clock clock = Clock.systemDefaultZone();

    @Schema(
            name = "dnsLookupBindAddress",
            description = "The Pulsar client dns lookup bind address, default behavior is bind on 0.0.0.0"
    )
    private String dnsLookupBindAddress = null;

    @Schema(
            name = "dnsLookupBindPort",
            description = "The Pulsar client dns lookup bind port, takes effect when dnsLookupBindAddress is "
                    + "configured, default value is 0."
    )
    private int dnsLookupBindPort = 0;

    @Schema(
            name = "dnsServerAddresses",
            description = "The Pulsar client dns lookup server address"
    )
    @SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
    private List<InetSocketAddress> dnsServerAddresses = new ArrayList<>();

    // socks5
    @Schema(
            name = "socks5ProxyAddress",
            description = "Address of SOCKS5 proxy."
    )
    private InetSocketAddress socks5ProxyAddress;

    @Schema(
            name = "socks5ProxyUsername",
            description = "User name of SOCKS5 proxy."
    )
    private String socks5ProxyUsername;

    @Schema(
            name = "socks5ProxyPassword",
            description = "Password of SOCKS5 proxy."
    )
    @Secret
    private String socks5ProxyPassword;

    @Schema(
            name = "socks5ProxyScope",
            description = "Selector that controls which connections go through the SOCKS5 proxy. "
                    + "BINARY_ONLY (default for PulsarClient) only routes Pulsar binary protocol connections; "
                    + "HTTP_ONLY only routes HTTP/HTTPS traffic (HTTP lookups, failover HTTP clients, admin REST); "
                    + "BOTH routes both. This preserves backward compatibility with the pre-existing behavior "
                    + "where the SOCKS5 proxy on PulsarClient only applied to the binary protocol."
    )
    private Socks5ProxyScope socks5ProxyScope = Socks5ProxyScope.BINARY_ONLY;

    @Schema(
            name = "description",
            description = "The extra description of the client version. The length cannot exceed 64."
    )
    private String description;

    private Map<String, String> lookupProperties;

    private transient OpenTelemetry openTelemetry;

    @Schema(
            name = "tracingEnabled",
            description = "Whether to enable OpenTelemetry distributed tracing. When enabled, "
                    + "tracing interceptors are automatically added to producers and consumers."
    )
    private boolean tracingEnabled = false;

    /**
     * Gets the authentication settings for the client.
     *
     * @return authentication settings for the client or {@link AuthenticationDisabled} when auth has not been specified
     */
    public Authentication getAuthentication() {
        return this.authentication != null ? this.authentication : AuthenticationDisabled.INSTANCE;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    public boolean isUseTls() {
        if (useTls) {
            return true;
        }
        if (getServiceUrl() != null
                && (this.getServiceUrl().startsWith("pulsar+ssl") || this.getServiceUrl().startsWith("https"))) {
            this.useTls = true;
            return true;
        }
        return false;
    }

    public long getLookupTimeoutMs() {
        if (lookupTimeoutMs >= 0) {
            return lookupTimeoutMs;
        } else {
            return operationTimeoutMs;
        }
    }

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }

    public InetSocketAddress getSocks5ProxyAddress() {
        if (Objects.nonNull(socks5ProxyAddress)) {
            return socks5ProxyAddress;
        }
        String proxyAddress = System.getProperty("socks5Proxy.address");
        return Optional.ofNullable(proxyAddress).map(address -> {
            try {
                URI uri = URI.create(address);
                return new InetSocketAddress(uri.getHost(), uri.getPort());
            } catch (Exception e) {
                throw new RuntimeException("Invalid config [socks5Proxy.address]", e);
            }
        }).orElse(null);
    }

    public String getSocks5ProxyUsername() {
        return Objects.nonNull(socks5ProxyUsername) ? socks5ProxyUsername : System.getProperty("socks5Proxy.username");
    }

    public String getSocks5ProxyPassword() {
        return Objects.nonNull(socks5ProxyPassword) ? socks5ProxyPassword : System.getProperty("socks5Proxy.password");
    }

    public void setLookupProperties(Map<String, String> lookupProperties) {
        this.lookupProperties = Collections.unmodifiableMap(lookupProperties);
    }

    public Map<String, String> getLookupProperties() {
        return (lookupProperties == null) ? Collections.emptyMap() : Collections.unmodifiableMap(lookupProperties);
    }
}
