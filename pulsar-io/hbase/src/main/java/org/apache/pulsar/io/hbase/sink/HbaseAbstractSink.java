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
import org.apache.pulsar.io.hbase.HbaseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Simple abstract class for Hbase sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class HbaseAbstractSink<T> implements Sink<T> {
    private HbaseSinkConfig hbaseSinkConfig;
    private Configuration configuration;
    private Connection connection;
    private Admin admin;
    private TableName tableName;
    private Table table;

    protected HbaseUtils.TableDefinition tableDefinition;

    // for flush
    private List<Record<T>> incomingList;
    private List<Record<T>> swapList;
    private AtomicBoolean isFlushing;
    private int timeoutMs;
    private int batchSize;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        hbaseSinkConfig = HbaseSinkConfig.load(config);
        if (hbaseSinkConfig.getZookeeperQuorum() == null
                || hbaseSinkConfig.getZookeeperClientPort() == null
                || hbaseSinkConfig.getTableName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        createTable(hbaseSinkConfig);
        tableDefinition = getTableDefinition(hbaseSinkConfig);

        timeoutMs = hbaseSinkConfig.getTimeoutMs();
        batchSize = hbaseSinkConfig.getBatchSize();

        incomingList = Lists.newArrayList();
        swapList = Lists.newArrayList();
        isFlushing = new AtomicBoolean(false);

        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(), timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        admin.close();
        connection.close();
    }

    @Override
    public void write(Record<T> record) throws Exception {
        int number;
        synchronized (incomingList) {
            incomingList.add(record);
            number = incomingList.size();
        }

        if (number == batchSize) {
            flushExecutor.schedule(() -> flush(), 0, TimeUnit.MILLISECONDS);
        }
    }

    // bind value with a PreparedStetement
    public abstract List<Put> bindValue(Record<T> message) throws Exception;

    private void flush() {
        // if not in flushing state, do flush, else return;
        if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("Starting flush, queue size: {}", incomingList.size());
            }
            if (!swapList.isEmpty()) {
                throw new IllegalStateException("swapList should be empty since last flush. swapList.size: " + swapList.size());
            }

            synchronized (incomingList) {
                List<Record<T>> tmpList;
                swapList.clear();

                tmpList = swapList;
                swapList = incomingList;
                incomingList = tmpList;
            }
            try {
                // bind each record value
                for (Record<T> record : swapList) {
                    List<Put> puts = bindValue(record);
                    if (CollectionUtils.isNotEmpty(puts)){
                        table.put(puts);
                    }
                    record.ack();
                }

                admin.flush(tableName);
                swapList.forEach(tRecord -> tRecord.ack());
            } catch (Exception e) {
                log.error("Got exception ", e);
                swapList.forEach(tRecord -> tRecord.fail());
            }

            // finish flush
            if (log.isDebugEnabled()) {
                log.debug("Finish flush, queue size: {}", swapList.size());
            }
            isFlushing.set(false);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Already in flushing state, will not flush, queue size: {}", incomingList.size());
            }
        }
    }

    private void createTable(HbaseSinkConfig hbaseSinkConfig) throws IOException {
        configuration = HBaseConfiguration.create();
        String hbaseConfigResources = hbaseSinkConfig.getHbaseConfigResources();
        if (StringUtils.isNotBlank(hbaseConfigResources)) {
            configuration.addResource(hbaseConfigResources);
        }

        configuration.set("hbase.zookeeper.quorum", hbaseSinkConfig.getZookeeperQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", hbaseSinkConfig.getZookeeperClientPort());
        configuration.set("hbase.master", hbaseSinkConfig.getHbaseMaster());

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        tableName = TableName.valueOf(hbaseSinkConfig.getTableName());
        if (!admin.tableExists(this.tableName)) {
            throw new IllegalArgumentException("Table does not exist.");
        }
        table = connection.getTable(this.tableName);
    }

    /**
     * Get the {@link HbaseUtils.TableDefinition} for the given table.
     */
    private HbaseUtils.TableDefinition getTableDefinition(HbaseSinkConfig hbaseSinkConfig) throws Exception {
        if (hbaseSinkConfig.getRowKeyName() == null
                || hbaseSinkConfig.getFamilyName() == null
                || hbaseSinkConfig.getQualifierNames() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
        return HbaseUtils.TableDefinition.of(hbaseSinkConfig.getRowKeyName(), hbaseSinkConfig.getFamilyName(), hbaseSinkConfig.getQualifierNames());
    }

}
