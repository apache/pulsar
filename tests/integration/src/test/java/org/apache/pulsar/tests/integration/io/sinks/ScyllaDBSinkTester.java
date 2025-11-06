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
package org.apache.pulsar.tests.integration.io.sinks;

import static org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase.randomName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.tests.integration.containers.ScyllaDBContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

/**
 * A tester for testing ScyllaDB sink.
 *
 * This class demonstrates ScyllaDB's drop-in compatibility with Apache Cassandra
 * by reusing the existing Cassandra sink connector (pulsar-io-cassandra) without
 * any modifications. ScyllaDB is protocol-compatible with Cassandra and works
 * seamlessly with the DataStax Cassandra driver.
 *
 * The test validates that:
 * - The Cassandra sink connector works identically with ScyllaDB
 * - No production code changes are needed
 * - The same CQL commands and data validation logic work
 * - ScyllaDB is truly a drop-in replacement for Cassandra in Pulsar
 */
@Slf4j
public class ScyllaDBSinkTester extends SinkTester<ScyllaDBContainer> {

    public static ScyllaDBSinkTester createTester(boolean builtin) {
        if (builtin) {
            return new ScyllaDBSinkTester(builtin);
        } else {
            return new ScyllaDBSinkTester();
        }
    }

    private static final String NAME = "scylladb";

    // Network alias matches the ScyllaDBContainer NAME
    private static final String ROOTS = "scylladb";
    private static final String KEY = "key";
    private static final String COLUMN = "col";

    // We use the SAME Cassandra connector archive
    private static final String ARCHIVE = "/pulsar/connectors/pulsar-io-cassandra-"
            + PulsarVersion.getVersion() + ".nar";

    private final String keySpace;
    private final String tableName;

    private Cluster cluster;
    private Session session;

    private ScyllaDBSinkTester() {
        // Use the SAME Cassandra sink class - key to drop-in compatibility
        super(NAME, ARCHIVE, "org.apache.pulsar.io.cassandra.CassandraStringSink");

        String suffix = randomName(8) + "_" + System.currentTimeMillis();
        this.keySpace = "keySpace_" + suffix;
        this.tableName = "tableName_" + suffix;

        // Configuration is identical to Cassandra, only the network alias differs
        sinkConfig.put("roots", ROOTS);
        sinkConfig.put("keyspace", keySpace);
        sinkConfig.put("columnFamily", tableName);
        sinkConfig.put("keyname", KEY);
        sinkConfig.put("columnName", COLUMN);
    }

    private ScyllaDBSinkTester(boolean builtin) {
        // Using SinkType.CASSANDRA because ScyllaDB is Cassandra-compatible
        super(NAME, SinkType.CASSANDRA);

        String suffix = randomName(8) + "_" + System.currentTimeMillis();
        this.keySpace = "keySpace_" + suffix;
        this.tableName = "tableName_" + suffix;

        sinkConfig.put("roots", ROOTS);
        sinkConfig.put("keyspace", keySpace);
        sinkConfig.put("columnFamily", tableName);
        sinkConfig.put("keyname", KEY);
        sinkConfig.put("columnName", COLUMN);
    }

    @Override
    protected ScyllaDBContainer createSinkService(PulsarCluster cluster) {
        return new ScyllaDBContainer(cluster.getClusterName());
    }

    @Override
    public void prepareSink() {
        // Build cluster connection using DataStax Cassandra driver
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(serviceContainer.getScyllaDBPort())
                .withoutJMXReporting()
                .build();

        session = cluster.connect();
        log.info("Connecting to ScyllaDB cluster at localhost:{}", serviceContainer.getScyllaDBPort());

        String createKeySpace =
                "CREATE KEYSPACE " + keySpace
                        + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}; ";
        log.info(createKeySpace);
        session.execute(createKeySpace);
        session.execute("USE " + keySpace);

        String createTable = "CREATE TABLE " + tableName
                + "(" + KEY + " text PRIMARY KEY, "
                + COLUMN + " text);";
        log.info(createTable);
        session.execute(createTable);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        // Same CQL as Cassandra tests
        String query = "SELECT * FROM " + tableName + ";";
        ResultSet result = session.execute(query);
        List<Row> rows = result.all();

        assertEquals(kvs.size(), rows.size());
        for (Row row : rows) {
            String key = row.getString(KEY);
            String value = row.getString(COLUMN);

            String expectedValue = kvs.get(key);
            assertNotNull(expectedValue);
            assertEquals(expectedValue, value);
        }
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }
}
