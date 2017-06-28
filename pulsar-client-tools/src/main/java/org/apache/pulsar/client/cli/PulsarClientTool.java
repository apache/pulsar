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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "Produce or consume messages on a specified topic")
public class PulsarClientTool {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);

    @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
    String serviceURL = null;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--auth-params" }, description = "Authentication parameters, e.g., \"key1:val1,key2:val2\".")
    String authParams = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    boolean useTls = false;
    boolean tlsAllowInsecureConnection = false;
    String tlsTrustCertsFilePath = null;

    JCommander commandParser;
    CmdProduce produceCommand;
    CmdConsume consumeCommand;

    public PulsarClientTool(Properties properties) throws MalformedURLException {
        this.serviceURL = StringUtils.isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility        
        if (StringUtils.isBlank(this.serviceURL)) {
            this.serviceURL = properties.getProperty("serviceUrl");
        }
        this.authPluginClassName = properties.getProperty("authPlugin");
        this.authParams = properties.getProperty("authParams");
        this.useTls = Boolean.parseBoolean(properties.getProperty("useTls"));
        this.tlsAllowInsecureConnection = Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();

        this.commandParser = new JCommander();
        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
        commandParser.addCommand("produce", produceCommand);
        commandParser.addCommand("consume", consumeCommand);
    }

    private void updateConfig() throws UnsupportedAuthenticationException, MalformedURLException {
        ClientConfiguration configuration = new ClientConfiguration();
        if (isNotBlank(this.authPluginClassName)) {
            configuration.setAuthentication(authPluginClassName, authParams);
        }
        configuration.setUseTls(this.useTls);
        configuration.setTlsAllowInsecureConnection(this.tlsAllowInsecureConnection);
        configuration.setTlsTrustCertsFilePath(this.tlsTrustCertsFilePath);

        this.produceCommand.updateConfig(this.serviceURL, configuration);
        this.consumeCommand.updateConfig(this.serviceURL, configuration);
    }

    public int run(String[] args) {
        try {
            if (isBlank(this.serviceURL)) {
                commandParser.usage();
                return -1;
            }

            commandParser.parse(args);
            if (help) {
                commandParser.usage();
                return 0;
            }

            try {
                this.updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                                     // it will default to the values passed in by the constructor
            } catch (MalformedURLException mue) {
                System.out.println("Unable to parse URL " + this.serviceURL);
                commandParser.usage();
                return -1;
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                commandParser.usage();
                return -1;
            }

            String chosenCommand = commandParser.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                return produceCommand.run();
            } else if ("consume".equals(chosenCommand)) {
                return consumeCommand.run();
            } else {
                commandParser.usage();
                return -1;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (e instanceof ParameterException) {
                commandParser.usage();
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
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                properties.load(fis);
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
        }

        PulsarClientTool clientTool = new PulsarClientTool(properties);
        int exit_code = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exit_code);

    }
}
