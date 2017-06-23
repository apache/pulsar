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
import static org.apache.pulsar.websocket.admin.WebSocketWebResource.ADMIN_PATH;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.admin.WebSocketProxyStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketServiceStarter {

    public static void main(String args[]) throws Exception {
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
        proxyServer.addRestResources(ADMIN_PATH, WebSocketProxyStats.class.getPackage().getName(), service);
        proxyServer.start();
        service.start();
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketServiceStarter.class);

}
