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
package com.yahoo.pulsar.admin.cli;

import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.ClientConfiguration;

public class PulsarAdminTool {
    private final JCommander jcommander;
    private final ClientConfiguration config;
    private URL url;

    @Parameter(names = { "--admin-url" }, description = "Admin Service URL to which to connect.")
    String serviceUrl = null;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--auth-params" }, description = "Authentication parameters, e.g., \"key1:val1,key2:val2\".")
    String authParams = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    PulsarAdminTool(Properties properties) throws Exception {
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        String serviceUrl = StringUtils.isNotBlank(properties.getProperty("webServiceUrl"))
                ? properties.getProperty("webServiceUrl") : properties.getProperty("serviceUrl");
        String authPluginClassName = properties.getProperty("authPlugin");
        String authParams = properties.getProperty("authParams");
        boolean useTls = Boolean.parseBoolean(properties.getProperty("useTls"));
        boolean tlsAllowInsecureConnection = Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection"));
        String tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

        url = null;
        try {
            url = new URL(serviceUrl);
        } catch (MalformedURLException e) {
            System.err.println("Invalid serviceUrl: '" + serviceUrl + "'");
            System.exit(1);
        }

        config = new ClientConfiguration();
        config.setAuthentication(authPluginClassName, authParams);
        config.setUseTls(useTls);
        config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
        config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

        PulsarAdmin admin = new PulsarAdmin(url, config);
        jcommander = new JCommander();
        jcommander.setProgramName("pulsar-admin");
        jcommander.addObject(this);
        jcommander.addCommand("clusters", new CmdClusters(admin));
        jcommander.addCommand("ns-isolation-policy", new CmdNamespaceIsolationPolicy(admin));
        jcommander.addCommand("brokers", new CmdBrokers(admin));
        jcommander.addCommand("broker-stats", new CmdBrokerStats(admin));
        jcommander.addCommand("properties", new CmdProperties(admin));
        jcommander.addCommand("namespaces", new CmdNamespaces(admin));
        jcommander.addCommand("persistent", new CmdPersistentTopics(admin));
        jcommander.addCommand("resource-quotas", new CmdResourceQuotas(admin));
    }

    JCommander getJCommander() {
        return jcommander;
    }

    boolean run(String[] args) {
        if (args.length == 0) {
            jcommander.usage();
            return false;
        }

        int cmdPos;
        for (cmdPos=0; cmdPos < args.length; cmdPos++) {
            if (jcommander.getCommands().containsKey(args[cmdPos])){
                break;
            }
        }

        try {
            jcommander.parse(Arrays.copyOfRange(args, 0, Math.min(cmdPos+1, args.length)));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            jcommander.usage();
            return false;
        }

        if (help) {
            jcommander.usage();
            return false;
        }

        if (jcommander.getParsedCommand() == null) {
            jcommander.usage();
            return false;
        } else {
            String cmd = jcommander.getParsedCommand();
            JCommander obj = jcommander.getCommands().get(cmd);
            CmdBase cmdObj = (CmdBase) obj.getObjects().get(0);
            if (StringUtils.isNotBlank(serviceUrl) || (StringUtils.isNotBlank(authPluginClassName) && StringUtils
                    .isNotBlank(authParams))) {
                if (StringUtils.isNotBlank(serviceUrl)) {
                    try {
                        url = new URL(serviceUrl);
                    } catch (MalformedURLException e) {
                        System.err.println("Invalid serviceUrl: '" + serviceUrl + "'");
                        System.exit(1);
                    }
                }
                try {
                    if (StringUtils.isNotBlank(authPluginClassName) && StringUtils.isNotBlank(authParams)) {
                        config.setAuthentication(authPluginClassName, authParams);
                    }
                    cmdObj = cmdObj.getClass().getConstructor(PulsarAdmin.class).newInstance(new PulsarAdmin(url, config));
                } catch (Exception e) {
                    System.err.println(e.getClass() + ": " + e.getMessage());
                    System.exit(1);
                }
            }

            return cmdObj.run(Arrays.copyOfRange(args, cmdPos+1, args.length));
        }
    }

    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                properties.load(fis);
            } finally {
                if (fis != null)
                    fis.close();
            }
        }

        PulsarAdminTool tool = new PulsarAdminTool(properties);

        if (tool.run(Arrays.copyOfRange(args, 1, args.length))) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
