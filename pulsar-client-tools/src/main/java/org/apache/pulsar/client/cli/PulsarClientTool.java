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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;


public class PulsarClientTool {

    @Getter
    @Parameters(commandDescription = "Produce or consume messages on a specified topic")
    public static class RootParams {
        @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
        String serviceURL = null;

        @Parameter(names = { "--proxy-url" }, description = "Proxy-server URL to which to connect.")
        String proxyServiceURL = null;

        @Parameter(names = { "--proxy-protocol" }, description = "Proxy protocol to select type of routing at proxy.")
        ProxyProtocol proxyProtocol = null;

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
        String authPluginClassName = null;

        @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
        String listenerName = null;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                    + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                    + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".")
        String authParams = null;

        @Parameter(names = { "-v", "--version" }, description = "Get version of pulsar client")
        boolean version;

        @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
        boolean help;

        @Parameter(names = { "--tlsTrustCertsFilePath" }, description = "File path to client trust certificates")
        String tlsTrustCertsFilePath;
    }

    protected RootParams rootParams;
    boolean tlsAllowInsecureConnection;
    boolean tlsEnableHostnameVerification;

    String tlsKeyFilePath;
    String tlsCertificateFilePath;


    // for tls with keystore type config
    boolean useKeyStoreTls;
    String tlsTrustStoreType;
    String tlsTrustStorePath;
    String tlsTrustStorePassword;
    String tlsKeyStoreType;
    String tlsKeyStorePath;
    String tlsKeyStorePassword;

    protected JCommander jcommander;
    IUsageFormatter usageFormatter;
    protected CmdProduce produceCommand;
    protected CmdConsume consumeCommand;
    protected CmdRead readCommand;
    CmdGenerateDocumentation generateDocumentation;

    public PulsarClientTool(Properties properties) {
        rootParams = new RootParams();
        initRootParamsFromProperties(properties);
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        this.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        this.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        this.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");

        this.tlsKeyStoreType = properties.getProperty("tlsKeyStoreType", "JKS");
        this.tlsKeyStorePath = properties.getProperty("tlsKeyStorePath");
        this.tlsKeyStorePassword = properties.getProperty("tlsKeyStorePassword");
        this.tlsKeyFilePath = properties.getProperty("tlsKeyFilePath");
        this.tlsCertificateFilePath = properties.getProperty("tlsCertificateFilePath");

        initJCommander();
    }

    protected void initJCommander() {
        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();
        readCommand = new CmdRead();
        generateDocumentation = new CmdGenerateDocumentation();

        this.jcommander = new JCommander();
        this.usageFormatter = new DefaultUsageFormatter(this.jcommander);
        jcommander.setProgramName("pulsar-client");
        jcommander.addObject(rootParams);
        jcommander.addCommand("produce", produceCommand);
        jcommander.addCommand("consume", consumeCommand);
        jcommander.addCommand("read", readCommand);
        jcommander.addCommand("generate_documentation", generateDocumentation);
    }

    protected void initRootParamsFromProperties(Properties properties) {
        this.rootParams.serviceURL = isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (isBlank(this.rootParams.serviceURL)) {
            this.rootParams.serviceURL = properties.getProperty("serviceUrl");
        }
        this.rootParams.authPluginClassName = properties.getProperty("authPlugin");
        this.rootParams.authParams = properties.getProperty("authParams");
        this.rootParams.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");
        this.rootParams.proxyServiceURL = StringUtils.trimToNull(properties.getProperty("proxyServiceUrl"));
        String proxyProtocolString = StringUtils.trimToNull(properties.getProperty("proxyProtocol"));
        if (proxyProtocolString != null) {
            try {
                this.rootParams.proxyProtocol = ProxyProtocol.valueOf(proxyProtocolString.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Incorrect proxyProtocol name '" + proxyProtocolString + "'");
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private void updateConfig() throws UnsupportedAuthenticationException {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(0, SizeUnit.BYTES);
        Authentication authentication = null;
        if (isNotBlank(this.rootParams.authPluginClassName)) {
            authentication = AuthenticationFactory.create(rootParams.authPluginClassName, rootParams.authParams);
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.rootParams.listenerName)) {
            clientBuilder.listenerName(this.rootParams.listenerName);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.enableTlsHostnameVerification(this.tlsEnableHostnameVerification);
        clientBuilder.serviceUrl(rootParams.serviceURL);

        clientBuilder.tlsTrustCertsFilePath(this.rootParams.tlsTrustCertsFilePath)
                .tlsKeyFilePath(tlsKeyFilePath)
                .tlsCertificateFilePath(tlsCertificateFilePath);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword)
                .tlsKeyStoreType(tlsKeyStoreType)
                .tlsKeyStorePath(tlsKeyStorePath)
                .tlsKeyStorePassword(tlsKeyStorePassword);

        if (isNotBlank(rootParams.proxyServiceURL)) {
            if (rootParams.proxyProtocol == null) {
                System.out.println("proxy-protocol must be provided with proxy-url");
                System.exit(1);
            }
            clientBuilder.proxyServiceUrl(rootParams.proxyServiceURL, rootParams.proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.readCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
    }

    public int run(String[] args) {
        try {
            jcommander.parse(args);

            if (isBlank(this.rootParams.serviceURL)) {
                jcommander.usage();
                return -1;
            }

            if (rootParams.version) {
                System.out.println("Current version of pulsar client is: " + PulsarVersion.getVersion());
                return 0;
            }

            if (rootParams.help) {
                jcommander.usage();
                return 0;
            }

            try {
                this.updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                                     // it will default to the values passed in by the constructor
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                exp.printStackTrace();
                return -1;
            }

            String chosenCommand = jcommander.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                return produceCommand.run();
            } else if ("consume".equals(chosenCommand)) {
                return consumeCommand.run();
            } else if ("read".equals(chosenCommand)) {
                return readCommand.run();
            } else if ("generate_documentation".equals(chosenCommand)) {
                return generateDocumentation.run();
            } else {
                jcommander.usage();
                return -1;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String chosenCommand = jcommander.getParsedCommand();
            if (e instanceof ParameterException) {
                try {
                    usageFormatter.usage(chosenCommand);
                } catch (ParameterException noCmd) {
                    e.printStackTrace();
                }
            } else {
                e.printStackTrace();
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: pulsar-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(1);
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        PulsarClientTool clientTool = new PulsarClientTool(properties);
        int exitCode = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exitCode);

    }
}
