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
package org.apache.pulsar.websocket.service;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ADMIN_PATH_V1;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ADMIN_PATH_V2;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ATTRIBUTE_PROXY_SERVICE_NAME;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketMultiTopicConsumerServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketReaderServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.admin.v1.WebSocketProxyStatsV1;
import org.apache.pulsar.websocket.admin.v2.WebSocketProxyStatsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;

public class WebSocketServiceStarter {
    @Command(name = "websocket", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class Arguments {
        @Parameters(description = "config file", arity = "0..1")
        private String configFile = "";

        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        try {
            commander.parseArgs(args);
            if (arguments.help) {
                commander.usage(commander.getOut());
                return;
            }
            if (arguments.generateDocs && arguments.configFile != null) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("websocket", commander);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            commander.getErr().println(e);
            return;
        }

        checkArgument(args.length == 1, "Need to specify a configuration file");
        try {
            // load config file and start proxy service
            String configFile = args[0];
            WebSocketProxyConfiguration config = loadConfig(configFile);
            ProxyServer proxyServer = new ProxyServer(config);
            WebSocketService service = new WebSocketService(config);
            start(proxyServer, service);
        } catch (Exception e) {
            log.error("Failed to start WebSocket service", e);
            ShutdownUtil.triggerImmediateForcefulShutdown();
        }
    }

    public static void start(ProxyServer proxyServer, WebSocketService service) throws Exception {
        proxyServer.addWebSocketServlet(WebSocketProducerServlet.SERVLET_PATH, new WebSocketProducerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketConsumerServlet.SERVLET_PATH, new WebSocketConsumerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketReaderServlet.SERVLET_PATH, new WebSocketReaderServlet(service));

        proxyServer.addWebSocketServlet(WebSocketProducerServlet.SERVLET_PATH_V2,
                new WebSocketProducerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketConsumerServlet.SERVLET_PATH_V2,
                new WebSocketConsumerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketMultiTopicConsumerServlet.SERVLET_PATH,
                new WebSocketMultiTopicConsumerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketReaderServlet.SERVLET_PATH_V2,
                new WebSocketReaderServlet(service));

        proxyServer.addRestResource(ADMIN_PATH_V1, ATTRIBUTE_PROXY_SERVICE_NAME, service, WebSocketProxyStatsV1.class);
        proxyServer.addRestResource(ADMIN_PATH_V2, ATTRIBUTE_PROXY_SERVICE_NAME, service, WebSocketProxyStatsV2.class);
        proxyServer.addRestResource("/", VipStatus.ATTRIBUTE_STATUS_FILE_PATH, service.getConfig().getStatusFilePath(),
                VipStatus.class);
        proxyServer.start();
        service.start();
    }

    private static WebSocketProxyConfiguration loadConfig(String configFile) throws Exception {
        log.info("Loading configuration from {}", configFile);
        WebSocketProxyConfiguration config = PulsarConfigurationLoader.create(configFile,
                WebSocketProxyConfiguration.class);
        PulsarConfigurationLoader.isComplete(config);
        return config;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketServiceStarter.class);

}
