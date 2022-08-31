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
package org.apache.pulsar.metadata.bookkeeper;

import static org.testng.Assert.assertEquals;
import java.net.URI;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.testng.annotations.Test;

/**
 * Unit test the static methods of {@link ZKMetadataDriverBase}.
 */
public class PulsarMetadataDriverBaseStaticTest {

    @Test
    public void testGetZKServersFromServiceUri() {
        String uriStr = "zk://server1;server2;server3/ledgers";
        URI uri = URI.create(uriStr);

        String zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
        assertEquals(zkServers, "server1,server2,server3");

        uriStr = "zk://server1,server2,server3/ledgers";
        uri = URI.create(uriStr);
        zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
        assertEquals(zkServers, "server1,server2,server3");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testResolveLedgerManagerFactoryNullUri() {
        ZKMetadataDriverBase.resolveLedgerManagerFactory(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testResolveLedgerManagerFactoryNullScheme() {
        ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("//127.0.0.1/ledgers"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testResolveLedgerManagerFactoryUnknownScheme() {
        ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("unknown://127.0.0.1/ledgers"));
    }

    @Test
    public void testResolveLedgerManagerFactoryUnspecifiedLayout() {
        assertEquals(ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk://127.0.0.1/ledgers")), null);
    }

    @Test
    public void testResolveLedgerManagerFactoryNullLayout() {
        assertEquals(ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk+null://127.0.0.1/ledgers")), null);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testResolveLedgerManagerFactoryFlat() {
        assertEquals(ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk+flat://127.0.0.1/ledgers")),
                org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testResolveLedgerManagerFactoryMs() {
        assertEquals(ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk+ms://127.0.0.1/ledgers")),
                org.apache.bookkeeper.meta.MSLedgerManagerFactory.class);
    }

    @Test
    public void testResolveLedgerManagerFactoryHierarchical() {
        assertEquals(
                ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk+hierarchical://127.0.0.1/ledgers")),
                HierarchicalLedgerManagerFactory.class);
    }

    @Test
    public void testResolveLedgerManagerFactoryLongHierarchical() {
        assertEquals(
                ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("zk+longhierarchical://127.0.0.1/ledgers")),
                LongHierarchicalLedgerManagerFactory.class
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testResolveLedgerManagerFactoryUnknownLedgerManagerFactory() {
        ZKMetadataDriverBase.resolveLedgerManagerFactory(
            URI.create("zk+unknown://127.0.0.1/ledgers"));
    }
}
