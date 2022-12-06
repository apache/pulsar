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
package org.apache.pulsar.io.jdbc;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    private Deque<Record<T>> incomingList;
    private AtomicBoolean isFlushing;
    private int batchSize;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        jdbcSinkConfig = JdbcSinkConfig.load(config);
        jdbcSinkConfig.validate();

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

        connection = DriverManager.getConnection(jdbcSinkConfig.getJdbcUrl(), properties);
        connection.setAutoCommit(!jdbcSinkConfig.isUseTransactions());
        log.info("Opened jdbc connection: {}, autoCommit: {}", jdbcUrl, connection.getAutoCommit());

        tableName = jdbcSinkConfig.getTableName();
        tableId = JdbcUtils.getTableId(connection, tableName);
        // Init PreparedStatement include insert, delete, update
        initStatement();

        int timeoutMs = jdbcSinkConfig.getTimeoutMs();
        batchSize = jdbcSinkConfig.getBatchSize();
        incomingList = new LinkedList<>();
        isFlushing = new AtomicBoolean(false);

        flushExecutor = Executors.newScheduledThreadPool(1);
        if (timeoutMs > 0) {
            flushExecutor.scheduleAtFixedRate(this::flush, timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private void initStatement()  throws Exception {
        List<String> keyList = getListFromConfig(jdbcSinkConfig.getKey());
        List<String> nonKeyList = getListFromConfig(jdbcSinkConfig.getNonKey());

        tableDefinition = JdbcUtils.getTableDefinition(connection, tableId,
                keyList, nonKeyList, jdbcSinkConfig.isExcludeNonDeclaredFields());
        insertStatement = connection.prepareStatement(generateInsertQueryStatement());

        if (jdbcSinkConfig.getInsertMode() == JdbcSinkConfig.InsertMode.UPSERT) {
            if (nonKeyList.isEmpty() || keyList.isEmpty()) {
                throw new IllegalStateException("UPSERT mode is not configured if 'key' and 'nonKey' "
                        + "config are not set.");
            }
            upsertStatement = connection.prepareStatement(generateUpsertQueryStatement());
        }
        if (!nonKeyList.isEmpty()) {
            updateStatement = connection.prepareStatement(generateUpdateQueryStatement());
        }
        if (!keyList.isEmpty()) {
            deleteStatement = connection.prepareStatement(generateDeleteQueryStatement());
        }
    }

    private static List<String> getListFromConfig(String jdbcSinkConfig) {
        List<String> nonKeyList = Lists.newArrayList();
        String nonKey = jdbcSinkConfig;
        if (nonKey != null && !nonKey.isEmpty()) {
            nonKeyList = Arrays.asList(nonKey.split(","));
        }
        return nonKeyList;
    }

    @Override
    public void close() throws Exception {
        if (flushExecutor != null) {
            int timeoutMs = jdbcSinkConfig.getTimeoutMs() * 2;
            flushExecutor.shutdown();
            flushExecutor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            flushExecutor = null;
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
        synchronized (incomingList) {
            incomingList.add(record);
            number = incomingList.size();
        }
        if (batchSize > 0 && number >= batchSize) {
            if (log.isDebugEnabled()) {
                log.debug("flushing by batches, hit batch size {}", batchSize);
            }
            flushExecutor.schedule(this::flush, 0, TimeUnit.MILLISECONDS);
        }
    }

    public String generateInsertQueryStatement() {
        return JdbcUtils.buildInsertSql(tableDefinition);
    }

    public String generateUpdateQueryStatement() {
        return JdbcUtils.buildUpdateSql(tableDefinition);
    }

    public abstract String generateUpsertQueryStatement();

    public abstract List<JdbcUtils.ColumnId> getColumnsForUpsert();

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


    private void flush() {
        if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
            boolean needAnotherRound;
            final Deque<Record<T>> swapList = new LinkedList<>();

            synchronized (incomingList) {
                if (log.isDebugEnabled()) {
                    log.debug("Starting flush, queue size: {}", incomingList.size());
                }
                final int actualBatchSize = batchSize > 0 ? Math.min(incomingList.size(), batchSize) :
                        incomingList.size();

                for (int i = 0; i < actualBatchSize; i++) {
                    swapList.add(incomingList.removeFirst());
                }
                needAnotherRound = batchSize > 0 && !incomingList.isEmpty() && incomingList.size() >= batchSize;
            }
            long start = System.nanoTime();

            int count = 0;
            try {
                PreparedStatement currentBatch = null;
                final List<Mutation> mutations = swapList
                        .stream()
                        .map(this::createMutation)
                        .collect(Collectors.toList());
                // bind each record value
                PreparedStatement statement;
                for (Mutation mutation : mutations) {
                    switch (mutation.getType()) {
                        case DELETE:
                            statement = deleteStatement;
                            break;
                        case UPDATE:
                            statement = updateStatement;
                            break;
                        case INSERT:
                            statement = insertStatement;
                            break;
                        case UPSERT:
                            statement = upsertStatement;
                            break;
                        default:
                            String msg = String.format(
                                    "Unsupported action %s, can be one of %s, or not set which indicate %s",
                                    mutation.getType(), Arrays.toString(MutationType.values()), MutationType.INSERT);
                            throw new IllegalArgumentException(msg);
                    }
                    bindValue(statement, mutation);
                    count += 1;
                    if (jdbcSinkConfig.isUseJdbcBatch()) {
                        if (currentBatch != null && statement != currentBatch) {
                            internalFlushBatch(swapList, currentBatch, count, start);
                            start = System.nanoTime();
                        }
                        statement.addBatch();
                        currentBatch = statement;
                    } else {
                        statement.execute();
                        if (!jdbcSinkConfig.isUseTransactions()) {
                            swapList.removeFirst().ack();
                        }
                    }
                }

                if (jdbcSinkConfig.isUseJdbcBatch()) {
                    internalFlushBatch(swapList, currentBatch, count, start);
                } else {
                    internalFlush(swapList);
                }
            } catch (Exception e) {
                log.error("Got exception {} after {} ms, failing {} messages",
                        e.getMessage(),
                        (System.nanoTime() - start) / 1000 / 1000,
                        swapList.size(),
                        e);
                swapList.forEach(Record::fail);
                try {
                    if (jdbcSinkConfig.isUseTransactions()) {
                        connection.rollback();
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            isFlushing.set(false);
            if (needAnotherRound) {
                flush();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Already in flushing state, will not flush, queue size: {}", incomingList.size());
            }
        }
    }

    private void internalFlush(Deque<Record<T>> swapList) throws SQLException {
        if (jdbcSinkConfig.isUseTransactions()) {
            connection.commit();
            swapList.forEach(Record::ack);
        }
    }

    private void internalFlushBatch(
            Deque<Record<T>> swapList,
            PreparedStatement currentBatch,
            int count,
            long start
    ) throws SQLException {
        executeBatch(swapList, currentBatch);
        if (log.isDebugEnabled()) {
            log.debug("Flushed {} messages in {} ms", count, (System.nanoTime() - start) / 1000 / 1000);
        }
    }

    private void executeBatch(Deque<Record<T>> swapList, PreparedStatement statement) throws SQLException {
        final int[] results = statement.executeBatch();
        Map<Integer, Integer> failuresMapping = null;
        final boolean useTransactions = jdbcSinkConfig.isUseTransactions();

        for (int r: results) {
            if (isBatchItemFailed(r)) {
                if (failuresMapping == null) {
                    failuresMapping = new HashMap<>();
                }
                final Integer current = failuresMapping.computeIfAbsent(r, code -> 1);
                failuresMapping.put(r, current + 1);
            }
        }
        if (failuresMapping == null || failuresMapping.isEmpty()) {
            if (useTransactions) {
                connection.commit();
            }
            for (int r: results) {
                swapList.removeFirst().ack();
            }
        } else {
            if (useTransactions) {
                connection.rollback();
            }
            for (int r: results) {
                swapList.removeFirst().fail();
            }
            String msg = "Batch failed, got error results (error_code->count): " + failuresMapping;
            // throwing an exception here means the main loop cycle will nack the messages in the next batch
            throw new SQLException(msg);
        }
    }

    private static boolean isBatchItemFailed(int returnCode) {
        if (returnCode == Statement.SUCCESS_NO_INFO || returnCode >= 0) {
            return false;
        }
        return true;
    }

}
