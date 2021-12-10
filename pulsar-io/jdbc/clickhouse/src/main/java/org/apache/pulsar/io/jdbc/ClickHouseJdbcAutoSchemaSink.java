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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
@Slf4j
@Connector(
    name = "jdbc-clickhouse",
    type = IOType.SINK,
    help = "A simple JDBC sink for ClickHouse that writes pulsar messages to a database table",
    configClass = JdbcSinkConfig.class
)
public class ClickHouseJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

	@Override
	public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
		ClickHouseProperties properties = new ClickHouseProperties();
		jdbcUrl = jdbcSinkConfig.getJdbcUrl();
		if (jdbcUrl == null) {
			throw new IllegalArgumentException("Required jdbc Url not set.");
		}
		String username = jdbcSinkConfig.getUserName();
		String password = jdbcSinkConfig.getPassword();

		if (username != null) {
			properties.setUser(jdbcSinkConfig.getUserName());
		}
		if (password != null) {
			properties.setPassword(jdbcSinkConfig.getPassword());
		}

		// keep connection stable
		properties.setConnectionTimeout(300000);
		properties.setSocketTimeout(300000);
		// load balance strategy
		BalancedClickhouseDataSource ckDataSource = new BalancedClickhouseDataSource(jdbcUrl, properties);
		// to check  clickhouse node  health
		ckDataSource.scheduleActualization(60, TimeUnit.SECONDS);
		final ClickHouseConnection ckConnection = ckDataSource.getConnection();
		super.connection = (ckConnection);
		ckConnection.setAutoCommit(false);

		log.info("Opened jdbc ckConnection: {}, autoCommit: {}", jdbcUrl, ckConnection.getAutoCommit());

		tableName = jdbcSinkConfig.getTableName();
		tableId = JdbcUtils.getTableId(ckConnection, tableName);
		// Init PreparedStatement include insert, delete, update
		initStatement();

		int timeoutMs = jdbcSinkConfig.getTimeoutMs();
		batchSize = jdbcSinkConfig.getBatchSize();
		incomingList = Lists.newArrayList();
		swapList = Lists.newArrayList();
		isFlushing = new AtomicBoolean(false);

		flushExecutor = Executors.newScheduledThreadPool(1);
		flushExecutor.scheduleAtFixedRate(this::flush, timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
	}
}
