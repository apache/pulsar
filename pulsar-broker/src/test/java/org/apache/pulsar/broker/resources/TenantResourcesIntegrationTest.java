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

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.testng.annotations.Test;

public class TenantResourcesIntegrationTest {

    @Test
    public void testActiveNamespaceCheckPreservesNamespaceReadFailure() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                ZKMetadataStore store = new ZKMetadataStore(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build(), false)) {
            String tenant = "tenant-" + UUID.randomUUID();
            String path = "/admin/policies/" + tenant + "/namespace";
            store.put(path, "{}".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get(5, TimeUnit.SECONDS);
            // Listing the parent still succeeds, but the namespace node cannot be read.
            store.getZkClient().setACL(path,
                    Collections.singletonList(new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.ANYONE_ID_UNSAFE)), -1);
            TenantResources resources = new TenantResources(store, 5);
            @Cleanup MetadataCache<?> cache = resources.getCache();

            Throwable failure = resources.hasActiveNamespace(tenant)
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(MetadataStoreException.class)
                    .hasCauseInstanceOf(KeeperException.NoAuthException.class);
        }
    }
}
