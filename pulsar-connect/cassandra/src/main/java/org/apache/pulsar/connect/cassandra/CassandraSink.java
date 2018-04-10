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

package org.apache.pulsar.connect.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.pulsar.common.util.KeyValue;
import org.apache.pulsar.connect.core.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Simple Cassandra sink
 * Takes in a KeyValue and writes it to a predefined keyspace/columnfamily/columnname.
 */
public class CassandraSink<K, V> implements Sink<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);

    // ----- Runtime fields
    private Cluster cluster;
    private Session session;
    CassandraSinkConfig cassandraSinkConfig;
    private PreparedStatement statement;

    @Override
    public void open(Map<String, String> config) throws Exception {
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
    public CompletableFuture<Void> write(KeyValue<K, V> tuple) {
        BoundStatement bound = statement.bind(tuple.getKey(), tuple.getValue());
        ResultSetFuture future = session.executeAsync(bound);
        CompletableFuture<Void> completable = new CompletableFuture<Void>();
        Futures.addCallback(future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        completable.complete(null);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completable.completeExceptionally(t);
                    }
                });
        return completable;
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
                b.withPort(Integer.valueOf(hostPort[1]));
            }
        }
        cluster = b.build();
        session = cluster.connect();
        session.execute("USE " + cassandraSinkConfig.getKeyspace());
    }
}