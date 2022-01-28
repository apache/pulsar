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
package org.apache.pulsar.tests.integration.io.sinks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.CassandraContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import java.util.List;
import java.util.Map;

import static org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase.randomName;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

/**
 * A tester for testing cassandra sink.
 */
@Slf4j
public class CassandraSinkTester extends SinkTester<CassandraContainer> {

    public static CassandraSinkTester createTester(boolean builtin) {
        if (builtin) {
            return new CassandraSinkTester(builtin);
        } else {
            return new CassandraSinkTester();
        }
    }

    private static final String NAME = "cassandra";

    private static final String ROOTS = "cassandra";
    private static final String KEY = "key";
    private static final String COLUMN = "col";

    private final String keySpace;
    private final String tableName;

    private Cluster cluster;
    private Session session;

    private CassandraSinkTester() {
        super(NAME, "/pulsar/connectors/pulsar-io-cassandra-2.2.0-incubating-SNAPSHOT.nar", "org.apache.pulsar.io.cassandra.CassandraStringSink");

        String suffix = randomName(8) + "_" + System.currentTimeMillis();
        this.keySpace = "keySpace_" + suffix;
        this.tableName = "tableName_" + suffix;

        sinkConfig.put("roots", ROOTS);
        sinkConfig.put("keyspace", keySpace);
        sinkConfig.put("columnFamily", tableName);
        sinkConfig.put("keyname", KEY);
        sinkConfig.put("columnName", COLUMN);
    }

    private CassandraSinkTester(boolean builtin) {
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
    protected CassandraContainer createSinkService(PulsarCluster cluster) {
        return new CassandraContainer(cluster.getClusterName());
    }

    @Override
    public void prepareSink() {
        // build the sink
        cluster = Cluster.builder()
            .addContactPoint("localhost")
            .withPort(serviceContainer.getCassandraPort())
            .build();

        // connect to the cluster
        session = cluster.connect();
        log.info("Connecting to cassandra cluster at localhost:{}", serviceContainer.getCassandraPort());

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
}
