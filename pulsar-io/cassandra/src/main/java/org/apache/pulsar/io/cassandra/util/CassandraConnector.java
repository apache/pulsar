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
package org.apache.pulsar.io.cassandra.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.io.cassandra.CassandraSinkConfig;

public class CassandraConnector implements AutoCloseable {

    private Cluster cluster;
    private Session session;
    private PreparedStatement statement;
    private List<String> tableFields;
    private final CassandraSinkConfig config;

    public CassandraConnector(CassandraSinkConfig config) {
        this.config = config;
    }

    public void connect() {
        session = getCluster().connect(config.getKeyspace());
    }

    public synchronized Session getSession() {
        if (session == null || session.isClosed()) {
            this.connect();
        }
        return session;
    }

    public Metadata getTableMetadata() {
        return getCluster().getMetadata();
    }

    public PreparedStatement getPreparedStatement() {
        if (statement == null) {
            List<String> fields = getTableFields();

            StringBuilder sb = new StringBuilder("INSERT INTO ")
                    .append(config.getKeyspace() + "." + config.getColumnFamily() + " (");

            for (int idx = 0; idx < fields.size(); idx++) {
               sb.append(fields.get(idx));
               if (idx < fields.size() - 1) {
                   sb.append(", ");
               }
            }

            sb.append(") VALUES (");

            for (int idx = 0; idx < fields.size(); idx++) {
                sb.append("?");
                if (idx < fields.size() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(")");
            statement = getSession().prepare(sb.toString());
        }

        return statement;
    }

    List<String> getTableFields() {

        if (tableFields == null) {

            TableMetadata meta = getCluster().getMetadata()
                    .getKeyspace(config.getKeyspace())
                    .getTable(config.getColumnFamily());

            tableFields = new ArrayList<String>(meta.getColumns().size());

            for (ColumnMetadata col : meta.getColumns()) {
                tableFields.add(col.getName());
            }
        }
        return tableFields;
    }

    private synchronized Cluster getCluster() {
        if (cluster == null) {
            String[] hosts = config.getRoots().split(",");

            Cluster.Builder builder = Cluster.builder().withoutMetrics();

            for (int i = 0; i < hosts.length; ++i) {
                String[] hostPort = hosts[i].split(":");
                builder.addContactPoint(hostPort[0]);
                if (hostPort.length > 1) {
                    builder.withPort(Integer.parseInt(hostPort[1]));
                }
            }

            // Authenticate if credentials have been provided
            if (config.getUserName() != null
                    && config.getPassword() != null) {
                builder.withCredentials(
                        config.getUserName(),
                        config.getPassword()
                );
            }
            cluster = builder.build();
        }

        return cluster;
    }

    public void close() {
        getSession().close();
        getCluster().close();
    }
}
