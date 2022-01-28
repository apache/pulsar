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
package org.apache.pulsar.common.util.collections;

import static org.apache.pulsar.common.util.FieldParser.booleanToString;
import static org.apache.pulsar.common.util.FieldParser.convert;
import static org.apache.pulsar.common.util.FieldParser.integerToString;
import static org.apache.pulsar.common.util.FieldParser.stringToBoolean;
import static org.apache.pulsar.common.util.FieldParser.stringToDouble;
import static org.apache.pulsar.common.util.FieldParser.stringToList;
import static org.apache.pulsar.common.util.FieldParser.stringToLong;
import static org.apache.pulsar.common.util.FieldParser.stringToSet;
import static org.apache.pulsar.common.util.FieldParser.update;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

public class FieldParserTest {

    /**
     * test all conversion scenarios.
     *
     */
    @Test
    public void testConversion() {

        assertEquals(convert("2", Integer.class), Integer.valueOf(2));
        assertEquals(convert("2", Double.class), Double.valueOf(2));
        assertEquals(convert("2", Long.class), Long.valueOf(2));
        assertEquals(convert("true", Boolean.class), Boolean.TRUE);
        assertEquals(integerToString(1), String.valueOf(1));
        assertEquals(stringToList("1,2,3", Integer.class).get(2), Integer.valueOf(3));
        assertTrue(stringToSet("1,2,3", Integer.class).contains(3));
        assertEquals(stringToBoolean("true"), Boolean.TRUE);
        assertEquals(stringToDouble("2.2"), Double.valueOf(2.2));
        assertEquals(stringToLong("2"), Long.valueOf(2));
        assertEquals(booleanToString(Boolean.TRUE), String.valueOf(true));

        // test invalid value type
        try {
            convert("invalid", Long.class);
            fail("Should fail w/ conversion exception");
        } catch (Exception e) {
            // OK, expected
        }

        try {
            convert("1", Character.class);
            fail("Should fail w/ UnsupportedOperationException");
        } catch (UnsupportedOperationException iae) {
            // OK, expected
        }
    }

    /**
     * test object update from given properties.
     */
    @Test
    public void testUpdateObject() {
        final ServiceConfiguration config = new ServiceConfiguration();
        final String nameSpace = "ns1,ns2";
        final String zk = "localhost:2184";
        final Map<String, String> properties = new HashMap<String, String>() {
            {
                put("bootstrapNamespaces", nameSpace);
                put("zookeeperServers", zk);
            }
        };
        update(properties, config);
        assertEquals(config.getZookeeperServers(), zk);
        assertEquals(config.getBootstrapNamespaces().get(1), "ns2");
    }

    static class ServiceConfiguration {

        private String zookeeperServers;
        private List<String> bootstrapNamespaces = new ArrayList<>();

        public String getZookeeperServers() {
            return zookeeperServers;
        }

        public void setZookeeperServers(String zookeeperServers) {
            this.zookeeperServers = zookeeperServers;
        }

        public List<String> getBootstrapNamespaces() {
            return bootstrapNamespaces;
        }

        public void setBootstrapNamespaces(List<String> bootstrapNamespaces) {
            this.bootstrapNamespaces = bootstrapNamespaces;
        }

    }

}
