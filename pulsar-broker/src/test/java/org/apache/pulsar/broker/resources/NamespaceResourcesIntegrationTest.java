/*
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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.broker.resources.ResourceTestSupport.newNamespaceResources;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.io.Closer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.Test;

public class NamespaceResourcesIntegrationTest {

    @Test
    public void testDeleteNonEmptyNamespacePoliciesPreservesCallbackFailure() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                MetadataStore store = MetadataStoreFactory.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build());
                Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            resources.createPolicies(namespace, new Policies());
            store.put("/admin/policies/" + namespace + "/child", new byte[0], Optional.of(-1L))
                    .get(5, TimeUnit.SECONDS);

            Throwable failure = resources.deletePoliciesAsync(namespace)
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(MetadataStoreException.class)
                    .hasCauseInstanceOf(KeeperException.NotEmptyException.class);
        }
    }
}
