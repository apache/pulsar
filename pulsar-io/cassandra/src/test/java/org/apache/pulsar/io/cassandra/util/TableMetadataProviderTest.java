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

package org.apache.pulsar.io.cassandra.util;

import org.apache.pulsar.io.cassandra.CassandraSinkConfig;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableMetadataProviderTest {

    @Test
    @Ignore
    public final void getTableDefinitionTest() {

        CassandraSinkConfig config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");
        config.setColumnFamily("observation");
        config.setKeyspace("airquality");

        CassandraConnector connector = new CassandraConnector(config);

        TableMetadataProvider.TableDefinition table =
                TableMetadataProvider.getTableDefinition(
                        connector.getTableMetadata(),
                "airquality", "reading");

        assertNotNull(table);
        assertEquals(17, table.getColumns().size());
        assertEquals(1, table.getPartitionKeyColumns().size());
        assertEquals(1, table.getPrimaryKeyColumns().size());
    }
}
