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

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.Map;

public class CassandraSink implements Sink<GenericRecord> {

    private CassandraConnector connector;
    private CassandraSinkConfig cassandraSinkConfig;
    private PreparedStatement stmt;
    private BoundStatement boundStatement;

    @Override
    public void open(Map<String, Object> config, SinkContext ctx) throws Exception {

        cassandraSinkConfig = IOConfigUtils.loadWithSecrets(config, CassandraSinkConfig.class, ctx);

        if (cassandraSinkConfig.getRoots() == null
                || cassandraSinkConfig.getKeyspace() == null
                || cassandraSinkConfig.getColumnFamily() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        connector = new CassandraConnector(cassandraSinkConfig);
        connector.connect();
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {

        Object[] boundValues = new Object[connector.getTableFields().size()];
        GenericRecord generic = record.getValue();

        for (int idx = 0; idx < connector.getTableFields().size(); idx++) {
            String fieldName = connector.getTableFields().get(idx);
            boundValues[idx] = generic.getField(fieldName);
        }

        BoundStatement bs = getBoundStatement().bind(boundValues);

        ResultSetFuture future = connector.getSession().executeAsync(bs);

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

    @Override
    public void close() throws Exception {
        connector.close();
    }

    private PreparedStatement getStatement() {
        if (stmt == null) {
           stmt = connector.getPreparedStatement();
        }
        return stmt;
    }

    private BoundStatement getBoundStatement() {
        if (boundStatement == null) {
            boundStatement = getStatement().bind();
            boundStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        }
        return boundStatement;
    }
}
