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
package org.apache.pulsar.io.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.cassandra.util.BoundStatementProvider;
import org.apache.pulsar.io.cassandra.util.CassandraConnector;
import org.apache.pulsar.io.cassandra.util.RecordWrapper;
import org.apache.pulsar.io.cassandra.util.TableMetadataProvider;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "cassandra",
        type = IOType.SINK,
        help = "The CassandraStringSink is used for moving messages from Pulsar to Cassandra.",
        configClass = CassandraSinkConfig.class)
public abstract class CassandraAbstractSink<T> implements Sink<T> {

    CassandraConnector connector;
    CassandraSinkConfig cassandraSinkConfig;
    PreparedStatement stmt;
    BoundStatementProvider boundStatementProvider;

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

        boundStatementProvider = new BoundStatementProvider(
                TableMetadataProvider.getTableDefinition(
                        connector.getTableMetadata(),
                        cassandraSinkConfig.getKeyspace(),
                        cassandraSinkConfig.getColumnFamily()));
    }

    @Override
    public void write(Record<T> record) throws Exception {

        BoundStatement bs = boundStatementProvider.bindStatement(
                getStatement(), wrapRecord(record));

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
    public void close()  {
        if (connector != null) {
            try {
                connector.close();
            } catch (final Throwable t) {

            }
        }
    }

    abstract RecordWrapper<T> wrapRecord(Record<T> record);

    PreparedStatement getStatement() {
        if (stmt == null) {
            stmt = connector.getPreparedStatement();
        }
        return stmt;
    }

}
