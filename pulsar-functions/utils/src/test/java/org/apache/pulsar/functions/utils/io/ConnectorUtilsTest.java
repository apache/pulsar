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
package org.apache.pulsar.functions.utils.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.ConnectorMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConnectorUtilsTest {
    
    private static final String NEW_FORMAT_NAR_FILE_NAME = "pulsar-io-cassandra-new-format.nar";
    private static final String OLD_FORMAT_NAR_FILE_NAME = "pulsar-io-twitter-old-format.nar";
    
    private String NEW_FORMAT_FILE_PATH;
    private String OLD_FORMAT_FILE_PATH;
    
    @BeforeMethod
    public void setup() {
        URL file = Thread.currentThread().getContextClassLoader().getResource(NEW_FORMAT_NAR_FILE_NAME);
        if (file == null)  {
            throw new RuntimeException("Failed to file required test archive: " + NEW_FORMAT_NAR_FILE_NAME);
        }
        NEW_FORMAT_FILE_PATH = file.getFile();
        OLD_FORMAT_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(OLD_FORMAT_NAR_FILE_NAME).getFile();
    }

    /**
     * Validate that we can handle older versions of the pulsar-io.yaml
     * @throws IOException 
     */
    @Test
    public final void getConnectorsBackwardCompatTest() throws IOException {
        ConnectorMap map = ConnectorUtils.getConnectors(OLD_FORMAT_FILE_PATH);
        assertNotNull(map);
        assertNotNull(map.getConnectors());
        assertTrue(map.getConnectors().containsKey("twitter"));
        
        ConnectorDefinition def = map.getConnectors().get("twitter");
        assertNotNull(def);
        assertEquals(def.getName(), "twitter");
        assertEquals(def.getDescription(), "Ingest data from Twitter firehose");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.twitter.TwitterFireHose");
    }
    
    /**
     * Validate that we can parse the new pulsar-io.yaml format.
     */
    @Test
    public final void getConnectorsNewFormatTest() throws IOException {
        ConnectorMap map = ConnectorUtils.getConnectors(NEW_FORMAT_FILE_PATH);
        assertNotNull(map);
        assertNotNull(map.getConnectors());
        assertTrue(map.getConnectors().containsKey("cassandra"));
        
        ConnectorDefinition def = map.getConnectors().get("cassandra");
        assertNotNull(def);
        assertEquals(def.getName(), "cassandra");
        assertEquals(def.getDescription(), "Writes data into Cassandra");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.cassandra.CassandraStringSink");
    }
    
    /**
     * Validate that we can handle both versions simultaneously by loading in two
     * NAR file with different formats and confirming that connectors from both were loaded
     *
     */
    @Test
    public final void getConnectorsBothFormatTests() throws IOException {
        String narPath = NEW_FORMAT_FILE_PATH.
                substring(0,NEW_FORMAT_FILE_PATH.lastIndexOf(File.separator));
                
        Connectors connectors = ConnectorUtils.searchForConnectors(narPath);
        assertNotNull(connectors);
        assertNotNull(connectors.getConnectors());
        assertFalse(connectors.getConnectors().isEmpty());
        assertEquals(connectors.getConnectors().size(), 2);
        
        ConnectorDefinition def = connectors.getConnectors().get(0);
        assertNotNull(def);
        assertEquals(def.getName(), "cassandra");
        assertEquals(def.getDescription(), "Writes data into Cassandra");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.cassandra.CassandraStringSink");
        
        def = connectors.getConnectors().get(1);
        assertNotNull(def);
        assertEquals(def.getName(), "twitter");
        assertEquals(def.getDescription(), "Ingest data from Twitter firehose");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.twitter.TwitterFireHose");
    }
    
    /**
     * Validate that we can get a specific connector by name
     * @throws IOException 
     */
    @Test
    public final void getConnectorDefinitionTest() throws IOException {
        ConnectorDefinition def = ConnectorUtils.getConnectorDefinition("twitter", OLD_FORMAT_FILE_PATH);
        assertNotNull(def);
        assertEquals(def.getName(), "twitter");
        assertEquals(def.getDescription(), "Ingest data from Twitter firehose");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.twitter.TwitterFireHose");
        
        def = ConnectorUtils.getConnectorDefinition("cassandra", NEW_FORMAT_FILE_PATH);
        assertNotNull(def);
        assertEquals(def.getName(), "cassandra");
        assertEquals(def.getDescription(), "Writes data into Cassandra");
        assertEquals(def.getSourceClass(), null);
        assertEquals(def.getSinkClass(), "org.apache.pulsar.io.cassandra.CassandraStringSink");
    }
}
