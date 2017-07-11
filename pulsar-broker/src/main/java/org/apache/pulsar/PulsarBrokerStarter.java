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
package org.apache.pulsar;

import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.create;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;

import java.io.FileInputStream;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.aspectj.weaver.loadtime.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.ea.agentloader.AgentLoader;

public class PulsarBrokerStarter {

    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        ServiceConfiguration config = create((new FileInputStream(configFile)), ServiceConfiguration.class);
        // it validates provided configuration is completed
        isComplete(config);
        return config;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Need to specify a configuration file");
        }

        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
        });

        String configFile = args[0];
        ServiceConfiguration config = loadConfig(configFile);

        // load aspectj-weaver agent for instrumentation
        AgentLoader.loadAgentClass(Agent.class.getName(), null);
        
        @SuppressWarnings("resource")
        final PulsarService service = new PulsarService(config);
        Runtime.getRuntime().addShutdownHook(service.getShutdownService());

        try {
            service.start();
            log.info("PulsarService started");
        } catch (PulsarServerException e) {
            log.error("Failed to start pulsar service.", e);

            Runtime.getRuntime().halt(1);
        }

        service.waitUntilClosed();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}
