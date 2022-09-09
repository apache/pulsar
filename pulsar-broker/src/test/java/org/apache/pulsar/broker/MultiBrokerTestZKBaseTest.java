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
package org.apache.pulsar.broker;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * Multiple brokers with a real test Zookeeper server (instead of the mock server)
 */
@Slf4j
public abstract class MultiBrokerTestZKBaseTest extends MultiBrokerBaseTest {
    TestZKServer testZKServer;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        testZKServer = new TestZKServer();
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
        if (testZKServer != null) {
            try {
                testZKServer.close();
            } catch (Exception e) {
                log.error("Error in stopping ZK server", e);
            }
        }
    }

    @Override
    protected MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    @Override
    protected MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }
}
