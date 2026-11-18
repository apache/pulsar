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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class DynamicConfigurationResourcesTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void testSetDynamicConfigurationWithCreateReportsClosedStore(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            DynamicConfigurationResources resources = new DynamicConfigurationResources(store, 5);
            store.close();

            assertThatThrownBy(() -> resources.setDynamicConfigurationWithCreate(current -> Map.of("key", "value")))
                    .as(provider + ": updating dynamic configuration reports the closed store")
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }
}
