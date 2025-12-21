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

import org.apache.curator.framework.CuratorFramework;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.FlumeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticZooKeeperConfigurationProvider extends
        AbstractZooKeeperConfigurationProvider {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(StaticZooKeeperConfigurationProvider.class);

    public StaticZooKeeperConfigurationProvider(String agentName,
                                                String zkConnString, String basePath) {
        super(agentName, zkConnString, basePath);
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
        try {
            CuratorFramework cf = createClient();
            cf.start();
            try {
                byte[] data = cf.getData().forPath(basePath + "/" + getAgentName());
                return configFromBytes(data);
            } finally {
                cf.close();
            }
        } catch (Exception e) {
            LOGGER.error("Error getting configuration info from Zookeeper", e);
            throw new FlumeException(e);
        }
    }

}
