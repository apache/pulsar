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

package org.apache.pulsar.io.jdbc;

import static jersey.repackaged.com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Jdbc sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class JdbcAbstractSink<T> implements Sink<T> {
    // ----- Runtime fields
    private JdbcSinkConfig jdbcSinkConfig;
    @Getter
    private Connection connection;
    private String tableName;

    private JdbcUtils.TableId tableId;
    private PreparedStatement insertStatement;

    // TODO: turn to getSchema from SinkContext.getTopicSchema.getSchema(inputTopic)
    protected String schema;
    protected JdbcUtils.TableDefinition tableDefinition;

    // for flush
    private List<Record<T>> incomingList;
    private List<Record<T>> swapList;
    private AtomicBoolean isFlushing;
    private int timeoutMs;
    private int batchSize;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        jdbcSinkConfig = JdbcSinkConfig.load(config);

        String jdbcUrl = jdbcSinkConfig.getJdbcUrl();
        if (jdbcSinkConfig.getJdbcUrl() == null) {
            throw new IllegalArgumentException("Required jdbc Url not set.");
        }

        Properties properties = new Properties();
        String username = jdbcSinkConfig.getUserName();
        String password = jdbcSinkConfig.getPassword();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }

        connection = JdbcUtils.getConnection(jdbcUrl, properties);
        log.info("Connection opened");

        schema = jdbcSinkConfig.getSchema();
        tableName = jdbcSinkConfig.getTableName();
        tableId = JdbcUtils.getTableId(connection, tableName);
        tableDefinition = JdbcUtils.getTableDefinition(connection, tableId);
        insertStatement = JdbcUtils.buildInsertStatement(connection, JdbcUtils.buildInsertSql(tableDefinition));

        timeoutMs = jdbcSinkConfig.getTimeoutMs();
        batchSize = jdbcSinkConfig.getBatchSize();
        incomingList = Lists.newArrayList();
        swapList = Lists.newArrayList();
        isFlushing = new AtomicBoolean(false);

        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(), timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        flushExecutor.shutdown();
        if (connection != null) {
            connection.close();
        }
        log.info("Connection Closed");
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
    public abstract void bindValue(
        PreparedStatement statement,
        Record<T> message) throws Exception;


    private void flush() {
        // if not in flushing state, do flush, else return;
        if (isFlushing.compareAndSet(false, true)) {
            log.debug("Starting flush, queue size: {}", incomingList.size());
            checkState(swapList.isEmpty(),
                "swapList should be empty since last flush. swapList.size: " + swapList.size());

            synchronized (incomingList) {
                List<Record<T>> tmpList;
                swapList.clear();

                tmpList = swapList;
                swapList = incomingList;
                incomingList = tmpList;
            }

            int updateCount = 0;
            boolean noInfo = false;
            try {
                // bind each record value
                for (Record<T> record : swapList) {
                    bindValue(insertStatement, record);
                    insertStatement.addBatch();
                    record.ack();
                }

                for (int updates : insertStatement.executeBatch()) {
                    if (updates == Statement.SUCCESS_NO_INFO) {
                        noInfo = true;
                        continue;
                    }
                    updateCount += updateCount;
                }
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (Exception e) {
                log.error("Got exception ", e);
                swapList.forEach(tRecord -> tRecord.fail());
            }

            if (swapList.size() != updateCount) {
                log.error("Update count {}  not match total number of records {}", updateCount, swapList.size());
            }

            // finish flush
            log.debug("Finish flush, queue size: {}", swapList.size());
            isFlushing.set(false);
        } else {
            log.debug("Already in flushing state, will not flush, queue size: {}", incomingList.size());
        }
    }

}
