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
package org.apache.pulsar.common.naming;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.util.Codec;
import org.testng.annotations.Test;

@Test
public class DestinationNameTest {

    @Test
    void destination() {
        try {
            assertEquals(DestinationName.get("property.namespace:destination").getNamespace(), "property.namespace");
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getNamespace(),
                "property/cluster/namespace");
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getNamespace(),
                "property/cluster/namespace");

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination"),
                DestinationName.get("persistent", "property", "cluster", "namespace", "destination"));

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").hashCode(),
                DestinationName.get("persistent", "property", "cluster", "namespace", "destination").hashCode());

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").toString(),
                "persistent://property/cluster/namespace/destination");

        assertFalse(DestinationName.get("persistent://property/cluster/namespace/destination")
                .equals("persistent://property/cluster/namespace/destination"));

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getDomain(),
                DestinationDomain.persistent);
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getProperty(),
                "property");
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getCluster(),
                "cluster");
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getNamespacePortion(),
                "namespace");
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getNamespace(),
                "property/cluster/namespace");
        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination").getLocalName(),
                "destination");

        try {
            DestinationName.get("property.namespace:my-topic").getDomain();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property.namespace:my-topic").getProperty();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property.namespace:my-topic").getCluster();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property.namespace:my-topic").getNamespacePortion();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property.namespace:my-topic").getLocalName();
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property.namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("invalid://property/cluster/namespace/destination");
            fail("Should have raied exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("persistent://property/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property/cluster/namespace/destination");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("persistent:///cluster/namespace/mydest-1");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("persistent://pulsar//namespace/mydest-1");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("persistent://pulsar/cluster//mydest-1");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("persistent://pulsar/cluster/namespace/");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("://pulsar/cluster/namespace/");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        assertEquals(DestinationName.get("persistent://property/cluster/namespace/destination")
                .getPersistenceNamingEncoding(), "property/cluster/namespace/persistent/destination");

        try {
            DestinationName.get("property.namespace");
            fail("Should have raied exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            DestinationName.get("property/cluster/namespace");
            fail("Should have raied exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        DestinationName nameWithSlash = DestinationName.get("persistent://property/cluster/namespace/ns-abc/table/1");
        assertEquals(nameWithSlash.getEncodedLocalName(), Codec.encode("ns-abc/table/1"));

        DestinationName nameEndingInSlash = DestinationName
                .get("persistent://property/cluster/namespace/ns-abc/table/1/");
        assertEquals(nameEndingInSlash.getEncodedLocalName(), Codec.encode("ns-abc/table/1/"));

        DestinationName nameWithTwoSlashes = DestinationName
                .get("persistent://property/cluster/namespace//ns-abc//table//1//");
        assertEquals(nameWithTwoSlashes.getEncodedLocalName(), Codec.encode("/ns-abc//table//1//"));

        DestinationName nameWithRandomCharacters = DestinationName
                .get("persistent://property/cluster/namespace/$#3rpa/table/1");
        assertEquals(nameWithRandomCharacters.getEncodedLocalName(), Codec.encode("$#3rpa/table/1"));

        DestinationName dn = DestinationName.get("persistent://myprop/mycolo/myns/mytopic");
        assertEquals(dn.getPartition(0).toString(), "persistent://myprop/mycolo/myns/mytopic-partition-0");

        DestinationName partitionedDn = DestinationName.get("persistent://myprop/mycolo/myns/mytopic").getPartition(2);
        assertEquals(partitionedDn.getPartitionIndex(), 2);
        assertEquals(dn.getPartitionIndex(), -1);

        assertEquals(DestinationName.getPartitionIndex("persistent://myprop/mycolo/myns/mytopic-partition-4"), 4);
    }

    @Test
    public void testDecodeEncode() throws Exception {
        String encodedName = "a%3Aen-in_in_business_content_item_20150312173022_https%5C%3A%2F%2Fin.news.example.com%2Fr";
        String rawName = "a:en-in_in_business_content_item_20150312173022_https\\://in.news.example.com/r";
        assertEquals(Codec.decode(encodedName), rawName);
        assertEquals(Codec.encode(rawName), encodedName);

        String topicName = "persistent://prop/colo/ns/" + rawName;
        DestinationName name = DestinationName.get(topicName);

        assertEquals(name.getLocalName(), rawName);
        assertEquals(name.getEncodedLocalName(), encodedName);
        assertEquals(name.getPersistenceNamingEncoding(), "prop/colo/ns/persistent/" + encodedName);
    }
}
