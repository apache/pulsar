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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertNotEquals;

import lombok.Cleanup;

import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.Test;

public class CounterTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void basicTest(String provider, String url) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(url, MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store);

        long l1 = cs1.getNextCounterValue("/my/path").join();
        long l2 = cs1.getNextCounterValue("/my/path").join();
        long l3 = cs1.getNextCounterValue("/my/path").join();

        assertNotEquals(l1, l2);
        assertNotEquals(l2, l3);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store);

        long l4 = cs1.getNextCounterValue("/my/path").join();
        assertNotEquals(l3, l4);
    }
}
