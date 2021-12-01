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
package org.apache.pulsar.bookie.rackawareness;

import static org.testng.Assert.assertEquals;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BookieRackAffinityMappingTest {

    private BookieSocketAddress BOOKIE1 = null;
    private BookieSocketAddress BOOKIE2 = null;
    private BookieSocketAddress BOOKIE3 = null;
    private MetadataStore store;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory://local", MetadataStoreConfig.builder().build());
        BOOKIE1 = new BookieSocketAddress("127.0.0.1:3181");
        BOOKIE2 = new BookieSocketAddress("127.0.0.2:3181");
        BOOKIE3 = new BookieSocketAddress("127.0.0.3:3181");
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        store.close();
    }

    @Test
    public void testBasic() throws Exception {
        String data = "{\"group1\": {\"" + BOOKIE1
                + "\": {\"rack\": \"/rack0\", \"hostname\": \"bookie1.example.com\"}, \"" + BOOKIE2
                + "\": {\"rack\": \"/rack1\", \"hostname\": \"bookie2.example.com\"}}}";
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, data.getBytes(), Optional.empty()).join();

        // Case1: ZKCache is given
        BookieRackAffinityMapping mapping1 = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf1 = new ClientConfiguration();
        bkClientConf1.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping1.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping1.setConf(bkClientConf1);
        List<String> racks1 = mapping1
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks1.get(0), "/rack0");
        assertEquals(racks1.get(1), "/rack1");
        assertEquals(racks1.get(2), null);
    }

    @Test
    public void testNoBookieInfo() throws Exception {
        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), null);
        assertEquals(racks.get(1), null);
        assertEquals(racks.get(2), null);

        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("/rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("/rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertEquals(r.get(0), "/rack0");
            assertEquals(r.get(1), "/rack1");
            assertEquals(r.get(2), null);
        });

    }

    @Test
    public void testBookieInfoChange() throws Exception {
        Map<String, Map<BookieSocketAddress, BookieInfo>> bookieMapping = new HashMap<>();
        Map<BookieSocketAddress, BookieInfo> mainBookieGroup = new HashMap<>();

        mainBookieGroup.put(BOOKIE1, BookieInfo.builder().rack("rack0").build());
        mainBookieGroup.put(BOOKIE2, BookieInfo.builder().rack("rack1").build());

        bookieMapping.put("group1", mainBookieGroup);

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        BookieRackAffinityMapping mapping = new BookieRackAffinityMapping();
        ClientConfiguration bkClientConf = new ClientConfiguration();
        bkClientConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

        mapping.setBookieAddressResolver(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        mapping.setConf(bkClientConf);
        List<String> racks = mapping
                .resolve(Lists.newArrayList(BOOKIE1.getHostName(), BOOKIE2.getHostName(), BOOKIE3.getHostName()));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(racks.get(2), null);

        // add info for BOOKIE3 and check if the mapping picks up the change
        Map<BookieSocketAddress, BookieInfo> secondaryBookieGroup = new HashMap<>();
        secondaryBookieGroup.put(BOOKIE3, BookieInfo.builder().rack("rack0").build());

        bookieMapping.put("group2", secondaryBookieGroup);
        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookieMapping),
                Optional.empty()).join();

        racks = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        assertEquals(racks.get(0), "/rack0");
        assertEquals(racks.get(1), "/rack1");
        assertEquals(racks.get(2), "/rack0");

        store.put(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes(),
                Optional.empty()).join();

        Awaitility.await().untilAsserted(() -> {
            List<String> r = mapping.resolve(Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
            assertEquals(r.get(0), null);
            assertEquals(r.get(1), null);
            assertEquals(r.get(2), null);
        });
    }
}
