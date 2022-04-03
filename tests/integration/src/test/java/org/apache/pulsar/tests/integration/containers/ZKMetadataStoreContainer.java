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
package org.apache.pulsar.tests.integration.containers;

import org.apache.pulsar.metadata.impl.ZKMetadataStore;

/**
 * Zookeeper metadata store container.
 */
public class ZKMetadataStoreContainer implements MetadataStoreContainer {

    private ZKContainer zkContainer;

    public ZKMetadataStoreContainer(ZKContainer zkContainer) {
        this.zkContainer = zkContainer;
    }

    public ZKMetadataStoreContainer(String clusterName) {
        this(new ZKContainer(clusterName));
    }

    @Override
    public String getConnString(String host) {
        return ZKMetadataStore.ZK_SCHEME_IDENTIFIER + host + ":" + ZKContainer.ZK_PORT;
    }

    @Override
    public void start() {
        zkContainer.start();
    }

    @Override
    public void close() {
        zkContainer.close();
    }

    public ZKContainer getZkContainer() {
        return zkContainer;
    }
}