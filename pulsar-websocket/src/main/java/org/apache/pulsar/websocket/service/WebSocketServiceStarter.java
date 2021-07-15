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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ADMIN_PATH_V1;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ADMIN_PATH_V2;
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ATTRIBUTE_PROXY_SERVICE_NAME;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketPingPongServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketReaderServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.admin.v1.WebSocketProxyStatsV1;
import org.apache.pulsar.websocket.admin.v2.WebSocketProxyStatsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketServiceStarter {
    private static class Arguments {
        @Parameter(description = "config file")
        private String configFile = "";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String args[]) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(arguments);
            jcommander.parse(args);
            if (arguments.help) {
                jcommander.usage();
                return;
            }
            if (arguments.generateDocs && arguments.configFile != null) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("websocket", arguments);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            return;
        }

        checkArgument(args.length == 1, "Need to specify a configuration file");
        try {
            // load config file and start proxy service
            String configFile = args[0];
            log.info("Loading configuration from {}", configFile);
            WebSocketProxyConfiguration config = PulsarConfigurationLoader.create(configFile,
                    WebSocketProxyConfiguration.class);
            ProxyServer proxyServer = new ProxyServer(config);
            WebSocketService service = new WebSocketService(config);
            start(proxyServer, service);
        } catch (Exception e) {
            log.error("Failed to start WebSocket service", e);
            Runtime.getRuntime().halt(1);
        }
    }

    public static void start(ProxyServer proxyServer, WebSocketService service) throws Exception {
        proxyServer.addWebSocketServlet(WebSocketProducerServlet.SERVLET_PATH, new WebSocketProducerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketConsumerServlet.SERVLET_PATH, new WebSocketConsumerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketReaderServlet.SERVLET_PATH, new WebSocketReaderServlet(service));
        proxyServer.addWebSocketServlet(WebSocketPingPongServlet.SERVLET_PATH, new WebSocketPingPongServlet(service));

        proxyServer.addWebSocketServlet(WebSocketProducerServlet.SERVLET_PATH_V2, new WebSocketProducerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketConsumerServlet.SERVLET_PATH_V2, new WebSocketConsumerServlet(service));
        proxyServer.addWebSocketServlet(WebSocketReaderServlet.SERVLET_PATH_V2, new WebSocketReaderServlet(service));
        proxyServer.addWebSocketServlet(WebSocketPingPongServlet.SERVLET_PATH_V2, new WebSocketPingPongServlet(service));

        proxyServer.addRestResources(ADMIN_PATH_V1, WebSocketProxyStatsV1.class.getPackage().getName(), ATTRIBUTE_PROXY_SERVICE_NAME, service);
        proxyServer.addRestResources(ADMIN_PATH_V2, WebSocketProxyStatsV2.class.getPackage().getName(), ATTRIBUTE_PROXY_SERVICE_NAME, service);
        proxyServer.addRestResources("/", VipStatus.class.getPackage().getName(),
                VipStatus.ATTRIBUTE_STATUS_FILE_PATH, service.getConfig().getStatusFilePath());
        proxyServer.start();
        service.start();
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketServiceStarter.class);

}
