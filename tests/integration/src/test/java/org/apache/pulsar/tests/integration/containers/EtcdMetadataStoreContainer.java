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

import org.apache.pulsar.metadata.impl.EtcdMetadataStore;
import org.testcontainers.containers.Network;

/**
 * Etcd metadata store container.
 */
public class EtcdMetadataStoreContainer implements MetadataStoreContainer {

    private EtcdContainer etcdContainer;

    public EtcdMetadataStoreContainer(EtcdContainer etcdContainer) {
        this.etcdContainer = etcdContainer;
    }

    public EtcdMetadataStoreContainer(String clusterName, String networkAlias, Network network) {
        this(new EtcdContainer(clusterName, networkAlias, network));
    }

    @Override
    public String getConnString(String host) {
        return EtcdMetadataStore.ETCD_SCHEME_IDENTIFIER + "http://" + host + ":" + EtcdContainer.PORT;
    }

    @Override
    public void start() {
        etcdContainer.start();
    }

    @Override
    public void close() {
        etcdContainer.close();
    }

    public EtcdContainer getEtcdContainer() {
        return etcdContainer;
    }
}