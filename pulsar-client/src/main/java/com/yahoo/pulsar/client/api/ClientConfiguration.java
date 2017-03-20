/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.yahoo.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import com.yahoo.pulsar.client.impl.auth.AuthenticationDisabled;

/**
 * Class used to specify client side configuration like authentication, etc..
 *
 *
 */
public class ClientConfiguration implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private Authentication authentication = new AuthenticationDisabled();
    private long operationTimeoutMs = 30000;
    private long statsIntervalSeconds = 60;

    private int numIoThreads = 1;
    private int numListenerThreads = 1;
    private int connectionsPerBroker = 1;

    private boolean useTcpNoDelay = true;

    private boolean useTls = false;
    private String tlsTrustCertsFilePath = "";
    private boolean tlsAllowInsecureConnection = false;
    private int concurrentLookupRequest = 5000;
    private int maxNumberOfRejectedRequestPerConnection = 50;

    /**
     * @return the authentication provider to be used
     */
    public Authentication getAuthentication() {
        return authentication;
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration conf = new ClientConfiguration();
     * String authPluginClassName = "com.yahoo.pulsar.client.impl.auth.MyAuthentication";
     * String authParamsString = "key1:val1,key2:val2";
     * Authentication auth = AuthenticationFactory.create(authPluginClassName, authParamsString);
     * conf.setAuthentication(auth);
     * PulsarClient client = PulsarClient.create(serviceUrl, conf);
     * ....
     * </code>
     * </pre>
     *
     * @param authentication
     */
    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration conf = new ClientConfiguration();
     * String authPluginClassName = "com.yahoo.pulsar.client.impl.auth.MyAuthentication";
     * String authParamsString = "key1:val1,key2:val2";
     * conf.setAuthentication(authPluginClassName, authParamsString);
     * PulsarClient client = PulsarClient.create(serviceUrl, conf);
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    public void setAuthentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        this.authentication = AuthenticationFactory.create(authPluginClassName, authParamsString);
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration conf = new ClientConfiguration();
     * String authPluginClassName = "com.yahoo.pulsar.client.impl.auth.MyAuthentication";
     * Map<String, String> authParams = new HashMap<String, String>();
     * authParams.put("key1", "val1");
     * conf.setAuthentication(authPluginClassName, authParams);
     * PulsarClient client = PulsarClient.create(serviceUrl, conf);
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    public void setAuthentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        this.authentication = AuthenticationFactory.create(authPluginClassName, authParams);
    }

    /**
     * @return the operation timeout in ms
     */
    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    /**
     * Set the operation timeout <i>(default: 30 seconds)</i>
     * <p>
     * Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
     * operation will be maked as failed
     *
     * @param operationTimeout
     *            operation timeout
     * @param unit
     *            time unit for {@code operationTimeout}
     */
    public void setOperationTimeout(int operationTimeout, TimeUnit unit) {
        checkArgument(operationTimeout >= 0);
        this.operationTimeoutMs = unit.toMillis(operationTimeout);
    }

    /**
     * @return the number of threads to use for handling connections
     */
    public int getIoThreads() {
        return numIoThreads;
    }

    /**
     * Set the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>
     *
     * @param numIoThreads
     */
    public void setIoThreads(int numIoThreads) {
        checkArgument(numIoThreads > 0);
        this.numIoThreads = numIoThreads;
    }

    /**
     * @return the number of threads to use for message listeners
     */
    public int getListenerThreads() {
        return numListenerThreads;
    }

    /**
     * Set the number of threads to be used for message listeners <i>(default: 1 thread)</i>
     *
     * @param numListenerThreads
     */
    public void setListenerThreads(int numListenerThreads) {
        checkArgument(numListenerThreads > 0);
        this.numListenerThreads = numListenerThreads;
    }

    /**
     * @return the max number of connections per single broker
     */
    public int getConnectionsPerBroker() {
        return connectionsPerBroker;
    }

    /**
     * Sets the max number of connection that the client library will open to a single broker.
     * <p>
     * By default, the connection pool will use a single connection for all the producers and consumers. Increasing this
     * parameter may improve throughput when using many producers over a high latency connection.
     * <p>
     * Setting connections per broker to 0 will disable the connection pooling.
     *
     * @param connectionsPerBroker
     *            max number of connections
     */
    public void setConnectionsPerBroker(int connectionsPerBroker) {
        this.connectionsPerBroker = connectionsPerBroker;
    }

    /**
     * @return whether TCP no-delay should be set on the connections
     */
    public boolean isUseTcpNoDelay() {
        return useTcpNoDelay;
    }

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     * <p>
     * No-delay features make sure packets are sent out on the network as soon as possible, and it's critical to achieve
     * low latency publishes. On the other hand, sending out a huge number of small packets might limit the overall
     * throughput, so if latency is not a concern, it's advisable to set the <code>useTcpNoDelay</code> flag to false.
     * <p>
     * Default value is true
     *
     * @param useTcpNoDelay
     */
    public void setUseTcpNoDelay(boolean useTcpNoDelay) {
        this.useTcpNoDelay = useTcpNoDelay;
    }

    /**
     * @return whether TLS encryption is used on the connection
     */
    public boolean isUseTls() {
        return useTls;
    }

    /**
     * Configure whether to use TLS encryption on the connection <i>(default: false)</i>
     *
     * @param useTls
     */
    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    /**
     * @return path to the trusted TLS certificate file
     */
    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    /**
     * Set the path to the trusted TLS certificate file
     *
     * @param tlsTrustCertsFilePath
     */
    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    }

    /**
     * @return whether the Pulsar client accept untrusted TLS certificate from broker
     */
    public boolean isTlsAllowInsecureConnection() {
        return tlsAllowInsecureConnection;
    }

    /**
     * Configure whether the Pulsar client accept untrusted TLS certificate from broker <i>(default: false)</i>
     *
     * @param tlsAllowInsecureConnection
     */
    public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    }

    /**
     * Stats will be activated with positive statsIntervalSeconds
     * 
     * @return the interval between each stat info <i>(default: 60 seconds)</i>
     */
    public long getStatsIntervalSeconds() {
        return statsIntervalSeconds;
    }

    /**
     * Set the interval between each stat info <i>(default: 60 seconds)</i> Stats will be activated with positive
     * statsIntervalSeconds It should be set to at least 1 second
     * 
     * @param statsIntervalSeconds
     *            the interval between each stat info
     * @param unit
     *            time unit for {@code statsInterval}
     */
    public void setStatsInterval(long statsInterval, TimeUnit unit) {
        this.statsIntervalSeconds = unit.toSeconds(statsInterval);
    }

    /**
     * Get configured total allowed concurrent lookup-request.
     * 
     * @return
     */
    public int getConcurrentLookupRequest() {
        return concurrentLookupRequest;
    }

    /**
     * Number of concurrent lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 5000)</i> It should be configured with higher value only in case of it requires to produce/subscribe on
     * thousands of topic using created {@link PulsarClient}
     * 
     * @param concurrentLookupRequest
     */
    public void setConcurrentLookupRequest(int concurrentLookupRequest) {
        this.concurrentLookupRequest = concurrentLookupRequest;
    }

    /**
     * Get configured max number of reject-request in a time-frame (30 seconds) after which connection will be closed
     * 
     * @return
     */
    public int getMaxNumberOfRejectedRequestPerConnection() {
        return maxNumberOfRejectedRequestPerConnection;
    }

    /**
     * Set max number of broker-rejected requests in a certain time-frame (30 seconds) after which current connection
     * will be closed and client creates a new connection that give chance to connect a different broker <i>(default:
     * 50)</i>
     * 
     * @param maxNumberOfRejectedRequestPerConnection
     */
    public void setMaxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
        this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
    }

}
