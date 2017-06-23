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
package org.apache.pulsar.zookeeper;

import java.net.InetSocketAddress;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

public class ZooKeeperStarter {
    public static void main(String[] args) throws Exception {
        // Register basic JVM metrics
        DefaultExports.initialize();

        // Start Jetty to serve stats
        int port = Integer.parseInt(System.getProperties().getProperty("stats_server_port", "8080"));

        log.info("Starting ZK stats HTTP server at port {}", port);
        InetSocketAddress httpEndpoint = InetSocketAddress.createUnresolved("0.0.0.0", port);

        Server server = new Server(httpEndpoint);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        try {
            server.start();
        } catch (Exception e) {
            log.error("Failed to start HTTP server at port {}. Use \"-Dstats_server_port=1234\" to change port number",
                    port, e);
            throw e;
        }

        // Start the regular ZooKeeper server
        QuorumPeerMain.main(args);
    }

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperStarter.class);
}
