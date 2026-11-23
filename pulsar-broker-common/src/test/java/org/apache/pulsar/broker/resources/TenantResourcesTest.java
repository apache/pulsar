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

import static org.apache.pulsar.broker.resources.ResourceTestSupport.newMemoryStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.testng.annotations.Test;

public class TenantResourcesTest {

    @Test
    public void testListTenantsReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore()) {
            TenantResources resources = new TenantResources(store, 5);
            @Cleanup MetadataCache<?> cache = resources.getCache();
            store.close();

            assertThatThrownBy(resources::listTenants)
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testActiveNamespaceCheckIgnoresInvalidNamespaceData() throws Exception {
        try (MetadataStore store = newMemoryStore()) {
            TenantResources resources = new TenantResources(store, 5);
            @Cleanup MetadataCache<?> cache = resources.getCache();
            String tenant = "tenant-" + UUID.randomUUID();
            store.put("/admin/policies/" + tenant + "/legacy", "not-json".getBytes(StandardCharsets.UTF_8),
                    Optional.of(-1L)).get(5, TimeUnit.SECONDS);

            assertThat(resources.hasActiveNamespace(tenant))
                    .succeedsWithin(Duration.ofSeconds(5)).isNull();
        }
    }

    @Test
    public void testActiveNamespaceCheckReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore()) {
            TenantResources resources = new TenantResources(store, 5);
            @Cleanup MetadataCache<?> cache = resources.getCache();
            String tenant = "tenant-" + UUID.randomUUID();
            store.close();

            Throwable failure = resources.hasActiveNamespace(tenant)
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(AlreadyClosedException.class);
        }
    }
}
