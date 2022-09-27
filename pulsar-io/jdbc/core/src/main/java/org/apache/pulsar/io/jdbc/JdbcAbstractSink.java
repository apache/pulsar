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

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Jdbc sink.
 */
@Slf4j
public abstract class JdbcAbstractSink<T> implements Sink<T> {
    // ----- Runtime fields
    protected JdbcSinkConfig jdbcSinkConfig;
    @Getter
    private Connection connection;
    private String jdbcUrl;
    private String tableName;

    private JdbcUtils.TableId tableId;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;
    private PreparedStatement upsertStatement;
    private PreparedStatement deleteStatement;

    protected static final String ACTION_PROPERTY = "ACTION";

    protected JdbcUtils.TableDefinition tableDefinition;

    // for flush
    private List<Record<T>> incomingList;
    private List<Record<T>> swapList;
    private AtomicBoolean isFlushing;
    private int batchSize;

    private Flusher flusher;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        jdbcSinkConfig = JdbcSinkConfig.load(config);

        jdbcUrl = jdbcSinkConfig.getJdbcUrl();
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


        Class.forName(JdbcUtils.getDriverClassName(jdbcSinkConfig.getJdbcUrl()));
        connection = DriverManager.getConnection(jdbcSinkConfig.getJdbcUrl(), properties);
        connection.setAutoCommit(!jdbcSinkConfig.isUseTransactions());
        log.info("Opened jdbc connection: {}, autoCommit: {}", jdbcUrl, connection.getAutoCommit());

        tableName = jdbcSinkConfig.getTableName();
        tableId = JdbcUtils.getTableId(connection, tableName);
        // Init PreparedStatement include insert, delete, update
        initStatement();

        int timeoutMs = jdbcSinkConfig.getTimeoutMs();
        batchSize = jdbcSinkConfig.getBatchSize();
        incomingList = Lists.newArrayList();
        swapList = Lists.newArrayList();
        isFlushing = new AtomicBoolean(false);

        flusher = new Flusher(timeoutMs);
    }

    private void initStatement()  throws Exception {
        List<String> keyList = Lists.newArrayList();
        String key = jdbcSinkConfig.getKey();
        if (key != null && !key.isEmpty()) {
            keyList = Arrays.asList(key.split(","));
        }
        List<String> nonKeyList = Lists.newArrayList();
        String nonKey = jdbcSinkConfig.getNonKey();
        if (nonKey != null && !nonKey.isEmpty()) {
            nonKeyList = Arrays.asList(nonKey.split(","));
        }

        tableDefinition = JdbcUtils.getTableDefinition(connection, tableId, keyList, nonKeyList);
        insertStatement = JdbcUtils.buildInsertStatement(connection, generateInsertQueryStatement());
        if (jdbcSinkConfig.getInsertMode() == JdbcSinkConfig.InsertMode.UPSERT) {
            upsertStatement = JdbcUtils.buildInsertStatement(connection, generateUpsertQueryStatement());
        }
        if (!nonKeyList.isEmpty()) {
            updateStatement = JdbcUtils.buildUpdateStatement(connection, generateUpdateQueryStatement());
        }
        if (!keyList.isEmpty()) {
            deleteStatement = JdbcUtils.buildDeleteStatement(connection, generateDeleteQueryStatement());
        }
    }

    @Override
    public void close() throws Exception {
        if (flusher != null) {
            int timeoutMs = jdbcSinkConfig.getTimeoutMs() * 2;
            flusher.close();
            if (!flusher.latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                log.warn("Flusher cannot be closed in time.");
            }
            flusher = null;
        }
        if (insertStatement != null) {
            insertStatement.close();
        }
        if (updateStatement != null) {
            updateStatement.close();
        }
        if (upsertStatement != null) {
            upsertStatement.close();
        }
        if (deleteStatement != null) {
            deleteStatement.close();
        }
        if (connection != null && jdbcSinkConfig.isUseTransactions()) {
            connection.commit();
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
        log.info("Closed jdbc connection: {}", jdbcUrl);
    }

    @Override
    public void write(Record<T> record) throws Exception {
        int number;
        synchronized (this) {
            incomingList.add(record);
            number = incomingList.size();
        }
        if (number == batchSize) {
            flusher.scheduleNow();
        }
    }

    public String generateInsertQueryStatement() {
        return JdbcUtils.buildInsertSql(tableDefinition);
    }

    public String generateUpdateQueryStatement() {
        return JdbcUtils.buildUpdateSql(tableDefinition);
    }

    public abstract String generateUpsertQueryStatement();

    public String generateDeleteQueryStatement() {
        return JdbcUtils.buildDeleteSql(tableDefinition);
    }

    // bind value with a PreparedStetement
    public abstract void bindValue(
        PreparedStatement statement,
        Mutation mutation) throws Exception;

    public abstract Mutation createMutation(Record<T> message);

    @Data
    @AllArgsConstructor
    protected static class Mutation {
        private MutationType type;
        private Function<String, Object> values;
    }
    protected enum MutationType {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }

    private final class Flusher implements Runnable, Closeable {

        private volatile boolean closed = false;

        private final ScheduledExecutorService flushExecutor = Executors.newScheduledThreadPool(1);
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Phaser phaser = new Phaser(1) {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                latch.countDown();
                return true;
            }
        };

        public Flusher(int timeoutMs) {
            flushExecutor.scheduleAtFixedRate(this, timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
        }

        public void scheduleNow() {
            flushExecutor.schedule(this, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            phaser.register();
            if (closed) {
                phaser.arriveAndDeregister();
                return;
            }
            // if not in flushing state, do flush, else return;
            if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("Starting flush, queue size: {}", incomingList.size());
                }
                if (!swapList.isEmpty()) {
                    phaser.arriveAndDeregister();
                    throw new IllegalStateException("swapList should be empty since last flush. swapList.size: "
                            + swapList.size());
                }
                synchronized (this) {
                    List<Record<T>> tmpList;
                    swapList.clear();

                    tmpList = swapList;
                    swapList = incomingList;
                    incomingList = tmpList;
                }

                int count = 0;
                try {
                    // bind each record value
                    for (Record<T> record : swapList) {
                        final Mutation mutation = createMutation(record);
                        switch (mutation.getType()) {
                            case DELETE -> {
                                bindValue(deleteStatement, mutation);
                                count += 1;
                                deleteStatement.execute();
                            }
                            case UPDATE -> {
                                bindValue(updateStatement, mutation);
                                count += 1;
                                updateStatement.execute();
                            }
                            case INSERT -> {
                                bindValue(insertStatement, mutation);
                                count += 1;
                                insertStatement.execute();
                            }
                            case UPSERT -> {
                                bindValue(upsertStatement, mutation);
                                count += 1;
                                upsertStatement.execute();
                            }
                        }
                    }
                    if (jdbcSinkConfig.isUseTransactions()) {
                        connection.commit();
                    }
                    swapList.forEach(Record::ack);
                } catch (Exception e) {
                    log.error("Got exception {}", e.getMessage(), e);
                    swapList.forEach(Record::fail);
                    try {
                        if (jdbcSinkConfig.isUseTransactions()) {
                            connection.rollback();
                        }
                    } catch (Exception ex) {
                        phaser.arriveAndDeregister();
                        throw new RuntimeException(ex);
                    }
                }

                if (swapList.size() != count) {
                    log.error("Update count {} not match total number of records {}", count, swapList.size());
                }

                // finish flush
                if (log.isDebugEnabled()) {
                    log.debug("Finish flush, queue size: {}", swapList.size());
                }
                swapList.clear();
                isFlushing.set(false);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Already in flushing state, will not flush, queue size: {}", incomingList.size());
                }
            }
            phaser.arriveAndDeregister();
        }

        @Override
        public void close() {
            flushExecutor.shutdown();
            closed = true;
            phaser.arriveAndDeregister();
        }
    }

}
