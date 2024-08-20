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
package org.apache.pulsar.testclient;

import static org.apache.commons.lang3.StringUtils.isBlank;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.client.api.ProxyProtocol;
import picocli.CommandLine.Option;

/**
 * PerformanceBaseArguments contains common CLI arguments and parsing logic available to all sub-commands.
 * Sub-commands should create Argument subclasses and override the `validate` method as necessary.
 */
public abstract class PerformanceBaseArguments extends CmdBase{


    @Option(names = { "-u", "--service-url" }, description = "Pulsar Service URL", descriptionKey = "brokerServiceUrl")
    public String serviceURL;

    @Option(names = { "--auth-plugin" }, description = "Authentication plugin class name",
            descriptionKey = "authPlugin")
    public String authPluginClassName;

    @Option(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                    + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                    + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".", descriptionKey = "authParams")
    public String authParams;

    @Option(names = { "--ssl-factory-plugin" }, description = "Pulsar SSL Factory plugin class name",
            descriptionKey = "sslFactoryPlugin")
    public String sslfactoryPlugin;

    @Option(names = { "--ssl-factory-plugin-params" },
            description = "Pulsar SSL Factory Plugin parameters in the format: "
                    + "\"{\"key1\":\"val1\",\"key2\":\"val2\"}\".", descriptionKey = "sslFactoryPluginParams")
    public String sslFactoryPluginParams;

    @Option(names = {
            "--trust-cert-file" }, description = "Path for the trusted TLS certificate file",
            descriptionKey = "tlsTrustCertsFilePath")
    public String tlsTrustCertsFilePath = "";

    @Option(names = {
            "--tls-allow-insecure" }, description = "Allow insecure TLS connection",
            descriptionKey = "tlsAllowInsecureConnection")
    public Boolean tlsAllowInsecureConnection = null;

    @Option(names = {
            "--tls-enable-hostname-verification" }, description = "Enable TLS hostname verification",
            descriptionKey = "tlsEnableHostnameVerification")
    public Boolean tlsHostnameVerificationEnable = null;

    @Option(names = { "-c",
            "--max-connections" }, description = "Max number of TCP connections to a single broker")
    public int maxConnections = 1;

    @Option(names = { "-i",
            "--stats-interval-seconds" },
            description = "Statistics Interval Seconds. If 0, statistics will be disabled")
    public long statsIntervalSeconds = 0;

    @Option(names = {"-ioThreads", "--num-io-threads"}, description = "Set the number of threads to be "
            + "used for handling connections to brokers. The default value is 1.")
    public int ioThreads = 1;

    @Option(names = {"-bw", "--busy-wait"}, description = "Enable Busy-Wait on the Pulsar client")
    public boolean enableBusyWait = false;

    @Option(names = { "--listener-name" }, description = "Listener name for the broker.")
    public String listenerName = null;

    @Option(names = {"-lt", "--num-listener-threads"}, description = "Set the number of threads"
            + " to be used for message listeners")
    public int listenerThreads = 1;

    @Option(names = {"-mlr", "--max-lookup-request"}, description = "Maximum number of lookup requests allowed "
            + "on each broker connection to prevent overloading a broker")
    public int maxLookupRequest = 50000;

    @Option(names = { "--proxy-url" }, description = "Proxy-server URL to which to connect.",
            descriptionKey = "proxyServiceUrl")
    String proxyServiceURL = null;

    @Option(names = { "--proxy-protocol" }, description = "Proxy protocol to select type of routing at proxy.",
            descriptionKey = "proxyProtocol", converter = ProxyProtocolConverter.class)
    ProxyProtocol proxyProtocol = null;

    @Option(names = { "--auth_plugin" }, description = "Authentication plugin class name", hidden = true)
    public String deprecatedAuthPluginClassName;

    @Option(names = { "-ml", "--memory-limit", }, description = "Configure the Pulsar client memory limit "
            + "(eg: 32M, 64M)", converter = ByteUnitToLongConverter.class)
    public long memoryLimit;
    public PerformanceBaseArguments(String cmdName) {
        super(cmdName);
    }

    @Override
    public void validate() throws Exception {
        parseCLI();
    }

    /**
     * Parse the command line args.
     * @throws ParameterException If there is a problem parsing the arguments
     */
    public void parseCLI() {
        if (isBlank(authPluginClassName) && !isBlank(deprecatedAuthPluginClassName)) {
            authPluginClassName = deprecatedAuthPluginClassName;
        }
    }

}
