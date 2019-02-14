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
package org.apache.pulsar.io.flume;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Constants;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.util.SSLUtil;
import org.apache.pulsar.io.flume.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FlumeConnector {

    private static final Logger log = LoggerFactory
            .getLogger(FlumeConnector.class);

    protected Application application;

    public void StartConnector(FlumeConfig flumeConfig) throws Exception {
        SSLUtil.initGlobalSSLParameters();
        String agentName = flumeConfig.getName();
        boolean reload = !flumeConfig.getNoReloadConf();
        boolean isZkConfigured = false;
        if (flumeConfig.getZkConnString().length() > 0) {
            isZkConfigured = true;
        }
        if (isZkConfigured) {
            // get options
            String zkConnectionStr = flumeConfig.getZkConnString();
            String baseZkPath = flumeConfig.getZkBasePath();
            if (reload) {
                EventBus eventBus = new EventBus(agentName + "-event-bus");
                List<LifecycleAware> components = Lists.newArrayList();
                PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                        new PollingZooKeeperConfigurationProvider(
                                agentName, zkConnectionStr, baseZkPath, eventBus);
                components.add(zookeeperConfigurationProvider);
                application = new Application(components);
                eventBus.register(application);
            } else {
                StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                        new StaticZooKeeperConfigurationProvider(
                                agentName, zkConnectionStr, baseZkPath);
                application = new Application();
                application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
            }

        } else {
            File configurationFile = new File(flumeConfig.getConfFile());
             /*
         * The following is to ensure that by default the agent will fail on
         * startup if the file does not exist.
         */
            if (!configurationFile.exists()) {
                // If command line invocation, then need to fail fast
                if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
                        null) {
                    String path = configurationFile.getPath();
                    try {
                        path = configurationFile.getCanonicalPath();
                    } catch (IOException ex) {
                        log.error("Failed to read canonical path for file: " + path,
                                ex);
                    }
                    throw new ParseException("The specified configuration file does not exist: " + path);
                }
            }
            List<LifecycleAware> components = Lists.newArrayList();

            if (reload) {
                EventBus eventBus = new EventBus(agentName + "-event-bus");
                PollingPropertiesFileConfigurationProvider configurationProvider =
                        new PollingPropertiesFileConfigurationProvider(
                                agentName, configurationFile, eventBus, 30);
                components.add(configurationProvider);
                application = new Application(components);
                eventBus.register(application);
            } else {
                PropertiesFileConfigurationProvider configurationProvider =
                        new PropertiesFileConfigurationProvider(agentName, configurationFile);
                application = new Application();
                application.handleConfigurationEvent(configurationProvider.getConfiguration());
            }
        }
        application.start();

        final Application appReference = application;
        Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
            @Override
            public void run() {
                appReference.stop();
            }
        });
    }

    public void stop() {
        if (application != null) {
            application.stop();
        }
    }
}
