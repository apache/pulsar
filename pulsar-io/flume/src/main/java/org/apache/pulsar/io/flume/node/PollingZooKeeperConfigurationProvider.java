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
package org.apache.pulsar.io.flume.node;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.EventBus;

public class PollingZooKeeperConfigurationProvider extends
        AbstractZooKeeperConfigurationProvider implements LifecycleAware {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PollingZooKeeperConfigurationProvider.class);

    private final EventBus eventBus;

    private final CuratorFramework client;

    private NodeCache agentNodeCache;

    private FlumeConfiguration flumeConfiguration;

    private LifecycleState lifecycleState;

    public PollingZooKeeperConfigurationProvider(String agentName,
                                                 String zkConnString, String basePath, EventBus eventBus) {
        super(agentName, zkConnString, basePath);
        this.eventBus = eventBus;
        client = createClient();
        agentNodeCache = null;
        flumeConfiguration = null;
        lifecycleState = LifecycleState.IDLE;
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
        return flumeConfiguration;
    }

    @Override
    public void start() {
        LOGGER.debug("Starting...");
        try {
            client.start();
            try {
                agentNodeCache = new NodeCache(client, basePath + "/" + getAgentName());
                agentNodeCache.start();
                agentNodeCache.getListenable().addListener(() -> refreshConfiguration());
            } catch (Exception e) {
                client.close();
                throw e;
            }
        } catch (Exception e) {
            lifecycleState = LifecycleState.ERROR;
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlumeException(e);
            }
        }
        lifecycleState = LifecycleState.START;
    }

    private void refreshConfiguration() throws IOException {
        LOGGER.info("Refreshing configuration from ZooKeeper");
        byte[] data = null;
        ChildData childData = agentNodeCache.getCurrentData();
        if (childData != null) {
            data = childData.getData();
        }
        flumeConfiguration = configFromBytes(data);
        eventBus.post(getConfiguration());
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping...");
        if (agentNodeCache != null) {
            try {
                agentNodeCache.close();
            } catch (IOException e) {
                LOGGER.warn("Encountered exception while stopping", e);
                lifecycleState = LifecycleState.ERROR;
            }
        }

        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn("Error stopping Curator client", e);
            lifecycleState = LifecycleState.ERROR;
        }

        if (lifecycleState != LifecycleState.ERROR) {
            lifecycleState = LifecycleState.STOP;
        }
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }
}
