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

package org.apache.pulsar.io.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Map;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Cassandra sink
 * Users need to implement extractKeyValue function to use this sink
 */
public abstract class CassandraAbstractSink<K, V> implements Sink<byte[]> {

    // ----- Runtime fields
    private Cluster cluster;
    private Session session;
    CassandraSinkConfig cassandraSinkConfig;
    private PreparedStatement statement;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        cassandraSinkConfig = CassandraSinkConfig.load(config);
        if (cassandraSinkConfig.getRoots() == null
                || cassandraSinkConfig.getKeyspace() == null
                || cassandraSinkConfig.getKeyname() == null
                || cassandraSinkConfig.getColumnFamily() == null
                || cassandraSinkConfig.getColumnName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
        createClient(cassandraSinkConfig.getRoots());
        statement = session.prepare("INSERT INTO " + cassandraSinkConfig.getColumnFamily() + " ("
                + cassandraSinkConfig.getKeyname() + ", " + cassandraSinkConfig.getColumnName() + ") VALUES (?, ?)");
    }

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }

    @Override
    public void write(Record<byte[]> record) {
        KeyValue<K, V> keyValue = extractKeyValue(record);
        BoundStatement bound = statement.bind(keyValue.getKey(), keyValue.getValue());
        ResultSetFuture future = session.executeAsync(bound);
        Futures.addCallback(future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        record.ack();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        record.fail();
                    }
                }, MoreExecutors.directExecutor());
    }

    private void createClient(String roots) {
        String[] hosts = roots.split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid cassandra roots");
        }
        Cluster.Builder b = Cluster.builder();
        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            b.addContactPoint(hostPort[0]);
            if (hostPort.length > 1) {
                b.withPort(Integer.parseInt(hostPort[1]));
            }
        }
        cluster = b.build();
        session = cluster.connect();
        session.execute("USE " + cassandraSinkConfig.getKeyspace());
    }

    public abstract KeyValue<K, V> extractKeyValue(Record<byte[]> record);
}