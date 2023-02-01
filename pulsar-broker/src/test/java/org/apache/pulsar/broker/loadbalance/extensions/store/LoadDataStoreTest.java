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
package org.apache.pulsar.broker.loadbalance.extensions.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Test(groups = "broker")
public class LoadDataStoreTest extends MockedPulsarServiceBaseTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MyClass {
        String a;
        int b;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        createDefaultTenantInfo();
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(configClusterName)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPushGetAndRemove() throws Exception {

        String topic = TopicDomain.persistent + "://" + NamespaceName.SYSTEM_NAMESPACE + "/" + UUID.randomUUID();

        @Cleanup
        LoadDataStore<MyClass> loadDataStore =
                LoadDataStoreFactory.create(pulsar.getClient(), topic, MyClass.class);
        MyClass myClass1 = new MyClass("1", 1);
        loadDataStore.pushAsync("key1", myClass1).get();

        Awaitility.await().untilAsserted(() -> {
            assertTrue(loadDataStore.get("key1").isPresent());
            assertEquals(loadDataStore.get("key1").get(), myClass1);
        });
        assertEquals(loadDataStore.size(), 1);

        MyClass myClass2 = new MyClass("2", 2);
        loadDataStore.pushAsync("key2", myClass2).get();

        Awaitility.await().untilAsserted(() -> {
            assertTrue(loadDataStore.get("key2").isPresent());
            assertEquals(loadDataStore.get("key2").get(), myClass2);
        });
        assertEquals(loadDataStore.size(), 2);

        loadDataStore.removeAsync("key2").get();
        Awaitility.await().untilAsserted(() -> assertFalse(loadDataStore.get("key2").isPresent()));
        assertEquals(loadDataStore.size(), 1);

    }

    @Test
    public void testForEach() throws Exception {

        String topic = TopicDomain.persistent + "://" + NamespaceName.SYSTEM_NAMESPACE + "/" + UUID.randomUUID();

        @Cleanup
        LoadDataStore<Integer> loadDataStore =
                LoadDataStoreFactory.create(pulsar.getClient(), topic, Integer.class);

        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            Integer value = i;
            loadDataStore.pushAsync(key, value).get();
            map.put(key, value);
        }
        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.size(), 10));

        loadDataStore.forEach((key, value) -> {
            assertTrue(loadDataStore.get(key).isPresent());
            assertEquals(loadDataStore.get(key).get(), map.get(key));
        });

        assertEquals(loadDataStore.entrySet(), map.entrySet());
    }

}
