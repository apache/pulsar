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
package org.apache.pulsar.websocket.service;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WebSocketProxyConfiguration implements PulsarConfiguration {

    // Number of threads used by Proxy server
    public static final int PROXY_SERVER_EXECUTOR_THREADS = 2 * Runtime.getRuntime().availableProcessors();
    // Number of threads used by Websocket service
    public static final int WEBSOCKET_SERVICE_THREADS = 20;
    // Number of threads used by Global ZK
    public static final int GLOBAL_ZK_THREADS = 8;

    @FieldContext(required = true, doc = "Name of the cluster to which this broker belongs to")
    private String clusterName;

    @FieldContext(doc = "The HTTPS REST service URL to connect to broker")
    private String serviceUrl;

    @FieldContext(doc = "The HTTPS REST service TLS URL")
    private String serviceUrlTls;

    @FieldContext(doc = "The broker binary service URL (for produce and consume operations)")
    private String brokerServiceUrl;

    @FieldContext(doc = "The secured broker binary service URL (for produce and consume operations)")
    private String brokerServiceUrlTls;

    @FieldContext(doc = "Path for the file used to determine the rotation status for the broker "
            + "when responding to service discovery health checks")
    private String statusFilePath;

    @Deprecated
    @FieldContext(
            doc = "Configuration Store connection string",
            deprecated = true
    )
    private String globalZookeeperServers;

    @FieldContext(doc = "Connection string of configuration store servers")
    private String configurationStoreServers;

    @FieldContext(doc = "ZooKeeper session timeout in milliseconds")
    private long zooKeeperSessionTimeoutMillis = 30000;

    @FieldContext(doc = "ZooKeeper cache expiry time in seconds")
    private int zooKeeperCacheExpirySeconds = 300;

    @FieldContext(doc = "Port to use to server HTTP request")
    private Optional<Integer> webServicePort = Optional.of(8080);

    @FieldContext(doc = "Port to use to server HTTPS request")
    private Optional<Integer> webServicePortTls = Optional.empty();

    @FieldContext(doc = "Hostname or IP address the service binds on, default is 0.0.0.0.")
    private String bindAddress = "0.0.0.0";

    @FieldContext(doc = "Maximum size of a text message during parsing in WebSocket proxy")
    private int webSocketMaxTextFrameSize = 1024 * 1024;
    // --- Authentication ---
    @FieldContext(doc = "Enable authentication")
    private boolean authenticationEnabled;

    @FieldContext(doc = "Authentication provider name list, which is a list of class names")
    private Set<String> authenticationProviders = Sets.newTreeSet();

    @FieldContext(doc = "Enforce authorization")
    private boolean authorizationEnabled;

    @FieldContext(doc = "Authorization provider fully qualified class name")
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    @FieldContext(doc = "Role names that are treated as \"super-user\", "
            + "which means they can do all admin operations and publish to or consume from all topics")
    private Set<String> superUserRoles = Sets.newTreeSet();

    @FieldContext(doc = "Allow wildcard matching in authorization "
            + "(wildcard matching only applicable if wildcard-char: "
            + "presents at first or last position. For example: *.pulsar.service,pulsar.service.*)")
    private boolean authorizationAllowWildcardsMatching = false;

    @FieldContext(doc = "Proxy authentication settings used to connect to brokers")
    private String brokerClientAuthenticationPlugin;

    @FieldContext(doc = "Proxy authentication parameters used to connect to brokers")
    private String brokerClientAuthenticationParameters;

    @FieldContext(doc = "Path for the trusted TLS certificate file for outgoing connection to a server (broker)")
    private String brokerClientTrustCertsFilePath = "";

    @FieldContext(doc = "Number of IO threads in Pulsar client used in WebSocket proxy")
    private int webSocketNumIoThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(doc = "Number of threads to used in HTTP server")
    private int numHttpServerThreads = Math.max(6, Runtime.getRuntime().availableProcessors());

    @FieldContext(doc = "Number of connections per broker in Pulsar client used in WebSocket proxy")
    private int webSocketConnectionsPerBroker = Runtime.getRuntime().availableProcessors();

    @FieldContext(doc = "Timeout of idling WebSocket session (in milliseconds)")
    private int webSocketSessionIdleTimeoutMillis = 300000;

    @FieldContext(doc = "When this parameter is not empty, unauthenticated users perform as anonymousUserRole")
    private String anonymousUserRole = null;

    /***** --- TLS --- ****/
    @Deprecated
    private boolean tlsEnabled = false;

    @FieldContext(doc = "Enable TLS of broker client")
    private boolean brokerClientTlsEnabled = false;

    @FieldContext(doc = "Path for the TLS certificate file")
    private String tlsCertificateFilePath;

    @FieldContext(doc = "Path for the TLS private key file")
    private String tlsKeyFilePath;

    @FieldContext(doc = "Path for the trusted TLS certificate file")
    private String tlsTrustCertsFilePath = "";

    @FieldContext(doc = "Accept untrusted TLS certificate from client")
    private boolean tlsAllowInsecureConnection = false;

    @FieldContext(doc = "Specify whether client certificates are required for "
            + "TLS rejecting the connection if the client certificate is not trusted")
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    @FieldContext(doc = "TLS cert refresh duration (in seconds). 0 means checking every new connection.")
    private long tlsCertRefreshCheckDurationSec = 300;

    @FieldContext(doc = "Key-value properties. Types are all String")
    private Properties properties = new Properties();
}
