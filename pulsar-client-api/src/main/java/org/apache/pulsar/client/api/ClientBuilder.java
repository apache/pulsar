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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Builder interface that is used to configure and construct a {@link PulsarClient} instance.
 *
 * @since 2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ClientBuilder extends Serializable, Cloneable {

    /**
     * Construct the final {@link PulsarClient} instance.
     *
     * @return the new {@link PulsarClient} instance
     */
    PulsarClient build() throws PulsarClientException;

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     *
     * <pre>
     * {@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("serviceUrl", "pulsar://localhost:6650");
     * config.put("numIoThreads", 20);
     *
     * ClientBuilder builder = ...;
     * builder = builder.loadConf(config);
     *
     * PulsarClient client = builder.build();
     * }
     * </pre>
     *
     * @param config
     *            configuration to load
     * @return the client builder instance
     */
    ClientBuilder loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current client builder.
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>{@code
     * ClientBuilder builder = PulsarClient.builder()
     *               .ioThreads(8)
     *               .listenerThreads(4);
     *
     * PulsarClient client1 = builder.clone()
     *                  .serviceUrl("pulsar://localhost:6650").build();
     * PulsarClient client2 = builder.clone()
     *                  .serviceUrl("pulsar://other-host:6650").build();
     * }</pre>
     *
     * @return a clone of the client builder instance
     */
    ClientBuilder clone();

    /**
     * Configure the service URL for the Pulsar service.
     *
     * <p>This parameter is required.
     *
     * <p>Examples:
     * <ul>
     * <li>{@code pulsar://my-broker:6650} for regular endpoint</li>
     * <li>{@code pulsar+ssl://my-broker:6651} for TLS encrypted endpoint</li>
     * </ul>
     *
     * @param serviceUrl
     *            the URL of the Pulsar service that the client should connect to
     * @return the client builder instance
     */
    ClientBuilder serviceUrl(String serviceUrl);

    /**
     * Configure the service URL provider for Pulsar service.
     *
     * <p>Instead of specifying a static service URL string (with {@link #serviceUrl(String)}), an application
     * can pass a {@link ServiceUrlProvider} instance that dynamically provide a service URL.
     *
     * @param serviceUrlProvider
     *            the provider instance
     * @return the client builder instance
     */
    ClientBuilder serviceUrlProvider(ServiceUrlProvider serviceUrlProvider);

    /**
     * Configure the listenerName that the broker will return the corresponding `advertisedListener`.
     *
     * @param name the listener name
     * @return the client builder instance
     */
    ClientBuilder listenerName(String name);

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     *
     * <p>Example:
     * <pre>{@code
     * PulsarClient client = PulsarClient.builder()
     *         .serviceUrl("pulsar+ssl://broker.example.com:6651/")
     *         .authentication(
     *               AuthenticationFactory.TLS("/my/cert/file", "/my/key/file")
     *         .build();
     * }</pre>
     *
     * <p>For token based authentication, this will look like:
     * <pre>{@code
     * AuthenticationFactory
     *      .token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")
     * }</pre>
     *
     * @param authentication
     *            an instance of the {@link Authentication} provider already constructed
     * @return the client builder instance
     */
    ClientBuilder authentication(Authentication authentication);

    /**
     * Configure the authentication provider to use in the Pulsar client instance.
     *
     * <p>Example:
     * <pre>
     * <code>
     * PulsarClient client = PulsarClient.builder()
     *          .serviceUrl("pulsar+ssl://broker.example.com:6651/)
     *          .authentication(
     *              "org.apache.pulsar.client.impl.auth.AuthenticationTls",
     *              "tlsCertFile:/my/cert/file,tlsKeyFile:/my/key/file")
     *          .build();
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @return the client builder instance
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    ClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    /**
     * Configure the authentication provider to use in the Pulsar client instance
     * using a config map.
     *
     * <p>Example:
     * <pre>{@code
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * PulsarClient client = PulsarClient.builder()
     *          .serviceUrl("pulsar+ssl://broker.example.com:6651/)
     *          .authentication(
     *              "org.apache.pulsar.client.impl.auth.AuthenticationTls", conf)
     *          .build();
     * }</pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @return the client builder instance
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    ClientBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException;

    /**
     * Set the operation timeout <i>(default: 30 seconds)</i>.
     *
     * <p>Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
     * operation will be marked as failed
     *
     * @param operationTimeout
     *            operation timeout
     * @param unit
     *            time unit for {@code operationTimeout}
     * @return the client builder instance
     */
    ClientBuilder operationTimeout(int operationTimeout, TimeUnit unit);

    /**
     * Set lookup timeout <i>(default: matches operation timeout)</i>
     *
     * Lookup operations have a different load pattern to other operations. They can be handled by any broker, are not
     * proportional to throughput, and are harmless to retry. Given this, it makes sense to allow them to retry longer
     * than normal operation, especially if they experience a timeout.
     *
     * By default this is set to match operation timeout. This is to maintain legacy behaviour. However, in practice
     * it should be set to 5-10x the operation timeout.
     *
     * @param lookupTimeout
     *            lookup timeout
     * @param unit
     *            time unit for {@code lookupTimeout}
     * @return the client builder instance
     */
    ClientBuilder lookupTimeout(int lookupTimeout, TimeUnit unit);

    /**
     * Set the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>.
     *
     * @param numIoThreads the number of IO threads
     * @return the client builder instance
     */
    ClientBuilder ioThreads(int numIoThreads);

    /**
     * Set the number of threads to be used for message listeners <i>(default: 1 thread)</i>.
     *
     * <p>The listener thread pool is shared across all the consumers and readers that are
     * using a "listener" model to get messages. For a given consumer, the listener will be
     * always invoked from the same thread, to ensure ordering.
     *
     * @param numListenerThreads the number of listener threads
     * @return the client builder instance
     */
    ClientBuilder listenerThreads(int numListenerThreads);

    /**
     * Sets the max number of connection that the client library will open to a single broker.
     *
     * <p>By default, the connection pool will use a single connection for all the producers and consumers.
     * Increasing this parameter may improve throughput when using many producers over a high latency connection.
     *
     * @param connectionsPerBroker
     *            max number of connections per broker (needs to be greater than or equal to 0)
     * @return the client builder instance
     */
    ClientBuilder connectionsPerBroker(int connectionsPerBroker);

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     *
     * <p>No-delay features make sure packets are sent out on the network as soon as possible, and it's critical
     * to achieve low latency publishes. On the other hand, sending out a huge number of small packets
     * might limit the overall throughput, so if latency is not a concern,
     * it's advisable to set the <code>useTcpNoDelay</code> flag to false.
     *
     * <p>Default value is true.
     *
     * @param enableTcpNoDelay whether to enable TCP no-delay feature
     * @return the client builder instance
     */
    ClientBuilder enableTcpNoDelay(boolean enableTcpNoDelay);

    /**
     * Configure whether to use TLS encryption on the connection
     * <i>(default: true if serviceUrl starts with "pulsar+ssl://", false otherwise)</i>.
     *
     * @param enableTls
     * @deprecated use "pulsar+ssl://" in serviceUrl to enable
     * @return the client builder instance
     */
    @Deprecated
    ClientBuilder enableTls(boolean enableTls);

    /**
     * Set the path to the trusted TLS certificate file.
     *
     * @param tlsTrustCertsFilePath
     * @return the client builder instance
     */
    ClientBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath);

    /**
     * Configure whether the Pulsar client accept untrusted TLS certificate from broker <i>(default: false)</i>.
     *
     * @param allowTlsInsecureConnection whether to accept a untrusted TLS certificate
     * @return the client builder instance
     */
    ClientBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection);

    /**
     * It allows to validate hostname verification when client connects to broker over tls. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">RFC 818</a>
     *
     * @param enableTlsHostnameVerification whether to enable TLS hostname verification
     * @return the client builder instance
     */
    ClientBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification);

    /**
     * If Tls is enabled, whether use KeyStore type as tls configuration parameter.
     * False means use default pem type configuration.
     *
     * @param useKeyStoreTls
     * @return the client builder instance
     */
    ClientBuilder useKeyStoreTls(boolean useKeyStoreTls);

    /**
     * The name of the security provider used for SSL connections.
     * Default value is the default security provider of the JVM.
     *
     * @param sslProvider
     * @return the client builder instance
     */
    ClientBuilder sslProvider(String sslProvider);

    /**
     * The file format of the trust store file.
     *
     * @param tlsTrustStoreType
     * @return the client builder instance
     */
    ClientBuilder tlsTrustStoreType(String tlsTrustStoreType);

    /**
     * The location of the trust store file.
     *
     * @param tlsTrustStorePath
     * @return the client builder instance
     */
    ClientBuilder tlsTrustStorePath(String tlsTrustStorePath);

    /**
     * The store password for the key store file.
     *
     * @param tlsTrustStorePassword
     * @return the client builder instance
     */
    ClientBuilder tlsTrustStorePassword(String tlsTrustStorePassword);

    /**
     * A list of cipher suites.
     * This is a named combination of authentication, encryption, MAC and key exchange algorithm
     * used to negotiate the security settings for a network connection using TLS or SSL network protocol.
     * By default all the available cipher suites are supported.
     *
     * @param tlsCiphers
     * @return the client builder instance
     */
    ClientBuilder tlsCiphers(Set<String> tlsCiphers);

    /**
     * The SSL protocol used to generate the SSLContext.
     * Default setting is TLS, which is fine for most cases.
     * Allowed values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
     *
     * @param tlsProtocols
     * @return the client builder instance
     */
    ClientBuilder tlsProtocols(Set<String> tlsProtocols);

    /**
     * Configure a limit on the amount of direct memory that will be allocated by this client instance.
     * <p>
     * <b>Note: at this moment this is only limiting the memory for producers.</b>
     * <p>
     * Setting this to 0 will disable the limit.
     *
     * @param memoryLimit
     *            the limit
     * @param unit
     *            the memory limit size unit
     * @return the client builder instance
     */
    ClientBuilder memoryLimit(long memoryLimit, SizeUnit unit);

    /**
     * Set the interval between each stat info <i>(default: 60 seconds)</i> Stats will be activated with positive
     * statsInterval It should be set to at least 1 second.
     *
     * @param statsInterval
     *            the interval between each stat info
     * @param unit
     *            time unit for {@code statsInterval}
     * @return the client builder instance
     */
    ClientBuilder statsInterval(long statsInterval, TimeUnit unit);

    /**
     * Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
     * <i>(default: 5000)</i> It should be configured with higher value only in case of it requires to produce/subscribe
     * on thousands of topic using created {@link PulsarClient}.
     *
     * @param maxConcurrentLookupRequests
     * @return the client builder instance
     */
    ClientBuilder maxConcurrentLookupRequests(int maxConcurrentLookupRequests);

    /**
     * Number of max lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 50000)</i> It should be bigger than maxConcurrentLookupRequests.
     * Requests that inside maxConcurrentLookupRequests already send to broker, and requests beyond
     * maxConcurrentLookupRequests and under maxLookupRequests will wait in each client cnx.
     *
     * @param maxLookupRequests
     * @return the client builder instance
     */
    ClientBuilder maxLookupRequests(int maxLookupRequests);

    /**
     * Set the maximum number of times a lookup-request to a broker will be redirected.
     *
     * @since 2.6.0
     * @param maxLookupRedirects the maximum number of redirects
     * @return the client builder instance
     */
    ClientBuilder maxLookupRedirects(int maxLookupRedirects);

    /**
     * Set max number of broker-rejected requests in a certain time-frame (30 seconds) after which current connection
     * will be closed and client creates a new connection that give chance to connect a different broker <i>(default:
     * 50)</i>.
     *
     * @param maxNumberOfRejectedRequestPerConnection
     * @return the client builder instance
     */
    ClientBuilder maxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection);

    /**
     * Set keep alive interval for each client-broker-connection. <i>(default: 30 seconds)</i>.
     *
     * @param keepAliveInterval
     * @param unit the time unit in which the keepAliveInterval is defined
     * @return the client builder instance
     */
    ClientBuilder keepAliveInterval(int keepAliveInterval, TimeUnit unit);

    /**
     * Set the duration of time to wait for a connection to a broker to be established. If the duration passes without a
     * response from the broker, the connection attempt is dropped.
     *
     * @since 2.3.0
     * @param duration
     *            the duration to wait
     * @param unit
     *            the time unit in which the duration is defined
     * @return the client builder instance
     */
    ClientBuilder connectionTimeout(int duration, TimeUnit unit);

    /**
     * Set the duration of time for a backoff interval.
     *
     * @param duration the duration of the interval
     * @param unit the time unit in which the duration is defined
     * @return the client builder instance
     */
    ClientBuilder startingBackoffInterval(long duration, TimeUnit unit);

    /**
     * Set the maximum duration of time for a backoff interval.
     *
     * @param duration the duration of the interval
     * @param unit the time unit in which the duration is defined
     * @return the client builder instance
     */
    ClientBuilder maxBackoffInterval(long duration, TimeUnit unit);

    /**
     * Option to enable busy-wait settings. Default is false.
     *
     * <b>WARNING</b>: This option will enable spin-waiting on executors and IO threads in order to reduce latency
     * during context switches. The spinning will consume 100% CPU even when the broker is not doing any work. It
     * is recommended to reduce the number of IO threads and BK client threads to only have few CPU cores busy.
     *
     * @param enableBusyWait whether to enable busy wait
     * @return the client builder instance
     */
    ClientBuilder enableBusyWait(boolean enableBusyWait);

    /**
     * The clock used by the pulsar client.
     *
     * <p>The clock is currently used by producer for setting publish timestamps.
     * {@link Clock#millis()} is called to retrieve current timestamp as the publish
     * timestamp when producers produce messages. The default clock is a system default zone
     * clock. So the publish timestamp is same as calling {@link System#currentTimeMillis()}.
     *
     * <p>Warning: the clock is used for TTL enforcement and timestamp based seeks.
     * so be aware of the impacts if you are going to use a different clock.
     *
     * @param clock the clock used by the pulsar client to retrieve time information
     * @return the client builder instance
     */
    ClientBuilder clock(Clock clock);

    /**
     * Proxy-service url when client would like to connect to broker via proxy. Client can choose type of proxy-routing
     * using {@link ProxyProtocol}.
     *
     * @param proxyServiceUrl proxy service url
     * @param proxyProtocol   protocol to decide type of proxy routing eg: SNI-routing
     * @return
     */
    ClientBuilder proxyServiceUrl(String proxyServiceUrl, ProxyProtocol proxyProtocol);

    /**
     * If enable transaction, start the transactionCoordinatorClient with pulsar client.
     *
     * @param enableTransaction whether enable transaction feature
     * @return
     */
    ClientBuilder enableTransaction(boolean enableTransaction);

    /**
     * Set dns lookup bind address and port.
     * @param address dnsBindAddress
     * @param port dnsBindPort
     * @return
     */
    ClientBuilder dnsLookupBind(String address, int port);

    /**
     *  Set socks5 proxy address.
     * @param socks5ProxyAddress
     * @return
     */
    ClientBuilder socks5ProxyAddress(InetSocketAddress socks5ProxyAddress);

    /**
     *  Set socks5 proxy username.
     * @param socks5ProxyUsername
     * @return
     */
    ClientBuilder socks5ProxyUsername(String socks5ProxyUsername);

    /**
     *  Set socks5 proxy password.
     * @param socks5ProxyPassword
     * @return
     */
    ClientBuilder socks5ProxyPassword(String socks5ProxyPassword);
}
