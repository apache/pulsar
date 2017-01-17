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
package com.yahoo.pulsar.websocket.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.pulsar.common.util.FieldParser.update;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.websocket.WebSocketConsumerServlet;
import com.yahoo.pulsar.websocket.WebSocketProducerServlet;
import com.yahoo.pulsar.websocket.WebSocketService;

public class WebSocketServiceStarter {

    public static void main(String args[]) throws Exception {
        checkArgument(args.length == 1, "Need to specify a configuration file");
        try {
            // load config file and start proxy service
            String configFile = args[0];
            log.info("Loading configuration from {}", configFile);
            WebSocketProxyConfiguration config = load(configFile);
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
        proxyServer.start();
        service.start();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static WebSocketProxyConfiguration load(String configFile) throws IOException, IllegalArgumentException {
        final InputStream inStream = new FileInputStream(configFile);
        try {
            checkNotNull(inStream, "Unbable to read config file " + configFile);
            WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
            Properties properties = new Properties();
            properties.load(inStream);
            update((Map) properties, config);
            return config;
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketServiceStarter.class);

}
