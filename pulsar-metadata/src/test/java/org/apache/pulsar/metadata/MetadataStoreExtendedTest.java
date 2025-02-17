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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class MetadataStoreExtendedTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void sequentialKeys(String provider, Supplier<String> urlSupplier) throws Exception {
        final String basePath = newKey();

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        Stat stat1 = store.put(basePath, "value-1".getBytes(), Optional.of(-1L), EnumSet.of(CreateOption.Sequential))
                .join();
        assertNotNull(stat1);
        assertTrue(stat1.getVersion() >= 0L);
        assertTrue(stat1.isFirstVersion());
        assertNotEquals(stat1.getPath(), basePath);
        assertEquals(store.get(stat1.getPath()).join().get().getValue(), "value-1".getBytes());
        String seq1 = stat1.getPath().replace(basePath, "");
        long n1 = Long.parseLong(seq1);

        Stat stat2 = store.put(basePath, "value-2".getBytes(), Optional.of(-1L), EnumSet.of(CreateOption.Sequential))
                .join();
        assertNotNull(stat2);
        assertTrue(stat2.getVersion() >= 0L);
        assertTrue(stat2.isFirstVersion());
        assertNotEquals(stat2.getPath(), basePath);
        assertNotEquals(stat2.getPath(), stat1.getPath());
        assertEquals(store.get(stat2.getPath()).join().get().getValue(), "value-2".getBytes());
        String seq2 = stat2.getPath().replace(basePath, "");
        long n2 = Long.parseLong(seq2);

        assertNotEquals(seq1, seq2);
        assertTrue(n1 < n2);
    }

    @Test(dataProvider = "impl")
    public void testPersistentOrEphemeralPut(String provider, Supplier<String> urlSupplier) throws Exception {
        final String key1 = newKey();
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        store.put(key1, "value-1".getBytes(), Optional.empty(), EnumSet.noneOf(CreateOption.class)).join();
        var value = store.get(key1).join().get();
        assertEquals(value.getValue(), "value-1".getBytes());
        assertFalse(value.getStat().isEphemeral());
        assertTrue(value.getStat().isFirstVersion());
        var version = value.getStat().getVersion();

        store.put(key1, "value-2".getBytes(), Optional.empty(), EnumSet.noneOf(CreateOption.class)).join();
        value = store.get(key1).join().get();
        assertEquals(value.getValue(), "value-2".getBytes());
        assertFalse(value.getStat().isEphemeral());
        assertEquals(value.getStat().getVersion(), version + 1);

        final String key2 = newKey();
        store.put(key2, "value-4".getBytes(), Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();
        value = store.get(key2).join().get();
        assertEquals(value.getValue(), "value-4".getBytes());
        assertTrue(value.getStat().isEphemeral());
        assertTrue(value.getStat().isFirstVersion());
        version = value.getStat().getVersion();


        store.put(key2, "value-5".getBytes(), Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();
        value = store.get(key2).join().get();
        assertEquals(value.getValue(), "value-5".getBytes());
        assertTrue(value.getStat().isEphemeral());
        assertEquals(value.getStat().getVersion(), version + 1);
    }

}
