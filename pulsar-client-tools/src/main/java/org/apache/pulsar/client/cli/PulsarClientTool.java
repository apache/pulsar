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
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;

@Parameters(commandDescription = "Produce or consume messages on a specified topic")
public class PulsarClientTool {

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
                + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
    String authParams = null;

    @Parameter(names = { "-v", "--version" }, description = "Get version of pulsar client")
    boolean version;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    boolean tlsAllowInsecureConnection;
    boolean tlsEnableHostnameVerification;
    String tlsTrustCertsFilePath;

    // for tls with keystore type config
    boolean useKeyStoreTls;
    String tlsTrustStoreType;
    String tlsTrustStorePath;
    String tlsTrustStorePassword;

    JCommander commandParser;
    IUsageFormatter usageFormatter;
    CmdProduce produceCommand;
    CmdConsume consumeCommand;
    CmdGenerateDocumentation generateDocumentation;

    public PulsarClientTool(Properties properties) {
        this.serviceURL = isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (isBlank(this.serviceURL)) {
            this.serviceURL = properties.getProperty("serviceUrl");
        }
        this.authPluginClassName = properties.getProperty("authPlugin");
        this.authParams = properties.getProperty("authParams");
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

        this.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        this.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        this.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        this.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");

        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();
        generateDocumentation = new CmdGenerateDocumentation();

        this.commandParser = new JCommander();
        this.usageFormatter = new DefaultUsageFormatter(this.commandParser);
        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
        commandParser.addCommand("produce", produceCommand);
        commandParser.addCommand("consume", consumeCommand);
        commandParser.addCommand("generate_documentation", generateDocumentation);
    }

    private void updateConfig() throws UnsupportedAuthenticationException {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(0, SizeUnit.BYTES);
        Authentication authentication = null;
        if (isNotBlank(this.authPluginClassName)) {
            authentication = AuthenticationFactory.create(authPluginClassName, authParams);
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.listenerName)) {
            clientBuilder.listenerName(this.listenerName);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        clientBuilder.enableTlsHostnameVerification(this.tlsEnableHostnameVerification);
        clientBuilder.serviceUrl(serviceURL);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword);

        if (isNotBlank(proxyServiceURL)) {
            if (proxyProtocol == null) {
                System.out.println("proxy-protocol must be provided with proxy-url");
                System.exit(-1);
            }
            clientBuilder.proxyServiceUrl(proxyServiceURL, proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
    }

    public int run(String[] args) {
        try {
            commandParser.parse(args);

            if (isBlank(this.serviceURL)) {
                commandParser.usage();
                return -1;
            }

            if (version) {
                System.out.println("Current version of pulsar client is: " + PulsarVersion.getVersion());
                return 0;
            }

            if (help) {
                commandParser.usage();
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

            String chosenCommand = commandParser.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                return produceCommand.run();
            } else if ("consume".equals(chosenCommand)) {
                return consumeCommand.run();
            } else if ("generate_documentation".equals(chosenCommand)) {
                return generateDocumentation.run();
            } else {
                commandParser.usage();
                return -1;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String chosenCommand = commandParser.getParsedCommand();
            if (e instanceof ParameterException) {
                usageFormatter.usage(chosenCommand);
            } else {
                e.printStackTrace();
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: pulsar-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(-1);
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
