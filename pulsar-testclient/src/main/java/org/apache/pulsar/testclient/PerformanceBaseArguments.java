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
package org.apache.pulsar.testclient;

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.util.Properties;
import lombok.SneakyThrows;


public abstract class PerformanceBaseArguments {

    @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
    boolean help;

    @Parameter(names = { "-cf", "--conf-file" }, description = "Configuration file")
    public String confFile;

    @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
    public String serviceURL;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name")
    public String authPluginClassName;

    @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                    + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                    + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
    public String authParams;

    @Parameter(names = {
            "--trust-cert-file" }, description = "Path for the trusted TLS certificate file")
    public String tlsTrustCertsFilePath = "";

    @Parameter(names = {
            "--tls-allow-insecure" }, description = "Allow insecure TLS connection")
    public Boolean tlsAllowInsecureConnection = null;

    @Parameter(names = {
            "--tls-enable-hostname-verification" }, description = "Enable TLS hostname verification")
    public Boolean tlsHostnameVerificationEnable = null;

    @Parameter(names = { "-c",
            "--max-connections" }, description = "Max number of TCP connections to a single broker")
    public int maxConnections = 1;

    @Parameter(names = { "-i",
            "--stats-interval-seconds" },
            description = "Statistics Interval Seconds. If 0, statistics will be disabled")
    public long statsIntervalSeconds = 0;

    @Parameter(names = {"-ioThreads", "--num-io-threads"}, description = "Set the number of threads to be "
            + "used for handling connections to brokers. The default value is 1.")
    public int ioThreads = 1;

    @Parameter(names = {"-bw", "--busy-wait"}, description = "Enable Busy-Wait on the Pulsar client")
    public boolean enableBusyWait = false;

    @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
    public String listenerName = null;

    @Parameter(names = {"-lt", "--num-listener-threads"}, description = "Set the number of threads"
            + " to be used for message listeners")
    public int listenerThreads = 1;

    public abstract void fillArgumentsFromProperties(Properties prop);

    @SneakyThrows
    public void fillArgumentsFromProperties() {
        if (confFile == null) {
            return;
        }

        Properties prop = new Properties(System.getProperties());
        try (FileInputStream fis = new FileInputStream(confFile)) {
            prop.load(fis);
        }

        if (serviceURL == null) {
            serviceURL = prop.getProperty("brokerServiceUrl");
        }

        if (serviceURL == null) {
            serviceURL = prop.getProperty("webServiceUrl");
        }

        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (serviceURL == null) {
            serviceURL = prop.getProperty("serviceUrl", "http://localhost:8080/");
        }

        if (authPluginClassName == null) {
            authPluginClassName = prop.getProperty("authPlugin", null);
        }

        if (authParams == null) {
            authParams = prop.getProperty("authParams", null);
        }

        if (isBlank(tlsTrustCertsFilePath)) {
            tlsTrustCertsFilePath = prop.getProperty("tlsTrustCertsFilePath", "");
        }

        if (tlsAllowInsecureConnection == null) {
            tlsAllowInsecureConnection = Boolean.parseBoolean(prop
                    .getProperty("tlsAllowInsecureConnection", ""));
        }

        if (tlsHostnameVerificationEnable == null) {
            tlsHostnameVerificationEnable = Boolean.parseBoolean(prop
                    .getProperty("tlsEnableHostnameVerification", ""));

        }
        fillArgumentsFromProperties(prop);
    }

}
