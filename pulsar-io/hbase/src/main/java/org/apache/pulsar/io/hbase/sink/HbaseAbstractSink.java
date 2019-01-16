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
package org.apache.pulsar.io.hbase.sink;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A Simple abstract class for Hbase sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class HbaseAbstractSink<T> implements Sink<T> {

    @Data(staticConstructor = "of")
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class TableDefinition {
        private final String rowKeyName;
        private final String familyName;
        private final List<String> qualifierNames;
    }

    private HbaseSinkConfig hbaseSinkConfig;
    private Configuration configuration;
    private Connection connection;
    private Admin admin;
    private TableName tableName;
    private Table table;

    protected TableDefinition tableDefinition;

    // for flush
    private int batchTimeMs;
    private int batchSize;
    private long currentTime;
    private List<Record<T>> incomingList;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        hbaseSinkConfig = HbaseSinkConfig.load(config);
        if (hbaseSinkConfig.getZookeeperQuorum() == null
                || hbaseSinkConfig.getZookeeperClientPort() == null
                || hbaseSinkConfig.getTableName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        getTable(hbaseSinkConfig);
        tableDefinition = getTableDefinition(hbaseSinkConfig);

        batchTimeMs = hbaseSinkConfig.getBatchTimeMs();
        batchSize = hbaseSinkConfig.getBatchSize();
        currentTime = System.currentTimeMillis();
        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        if (null != admin) {
            admin.close();
        }

        if (null != connection) {
            connection.close();
        }

        if (null != flushExecutor) {
            flushExecutor.shutdown();
        }
    }

    @Override
    public void write(Record<T> record) throws Exception {
        synchronized (incomingList) {
            incomingList.add(record);

            if (System.currentTimeMillis() - currentTime > batchTimeMs * 1000
                    || incomingList.size() >= batchSize) {
                flushExecutor.schedule(() -> flush(), 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void flush() {
        try {
            List<Put> puts = new ArrayList<>();
            for (Record<T> value : incomingList) {
                bindValue(value, puts);
            }

            if (CollectionUtils.isNotEmpty(puts)) {
                table.put(puts);
            }
            admin.flush(tableName);
            incomingList.forEach(tRecord -> tRecord.ack());

            incomingList.clear();
            currentTime = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Got exception ", e);
            incomingList.forEach(tRecord -> tRecord.fail());
        }
    }

    // bind value with a PreparedStetement
    public abstract void bindValue(Record<T> message, List<Put> puts) throws Exception;

    private void getTable(HbaseSinkConfig hbaseSinkConfig) throws IOException {
        configuration = HBaseConfiguration.create();
        String hbaseConfigResources = hbaseSinkConfig.getHbaseConfigResources();
        if (StringUtils.isNotBlank(hbaseConfigResources)) {
            configuration.addResource(hbaseConfigResources);
        }

        configuration.set("hbase.zookeeper.quorum", hbaseSinkConfig.getZookeeperQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", hbaseSinkConfig.getZookeeperClientPort());
        configuration.set("zookeeper.znode.parent", hbaseSinkConfig.getZookeeperZnodeParent());

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        tableName = TableName.valueOf(hbaseSinkConfig.getTableName());
        if (!admin.tableExists(this.tableName)) {
            throw new IllegalArgumentException("Table does not exist.");
        }

        table = connection.getTable(this.tableName);
    }

    /**
     * Get the {@link TableDefinition} for the given table.
     */
    private TableDefinition getTableDefinition(HbaseSinkConfig hbaseSinkConfig) throws Exception {
        if (hbaseSinkConfig.getRowKeyName() == null
                || hbaseSinkConfig.getFamilyName() == null
                || hbaseSinkConfig.getQualifierNames() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
        return TableDefinition.of(hbaseSinkConfig.getRowKeyName(), hbaseSinkConfig.getFamilyName(), hbaseSinkConfig.getQualifierNames());
    }

}
