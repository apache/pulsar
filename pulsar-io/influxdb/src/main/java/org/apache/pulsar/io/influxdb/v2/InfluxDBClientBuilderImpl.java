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
package org.apache.pulsar.io.influxdb.v2;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import lombok.val;

public class InfluxDBClientBuilderImpl implements InfluxDBClientBuilder {

    @Override
    public InfluxDBClient build(InfluxDBSinkConfig influxDBSinkConfig) {
        val options = InfluxDBClientOptions.builder()
                .url(influxDBSinkConfig.getInfluxdbUrl())
                .authenticateToken(influxDBSinkConfig.getToken().toCharArray())
                .org(influxDBSinkConfig.getOrganization())
                .bucket(influxDBSinkConfig.getBucket())
                .logLevel(LogLevel.valueOf(influxDBSinkConfig.getLogLevel().toUpperCase()))
                .build();

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(options);

        if (!influxDBSinkConfig.isGzipEnable()) {
            return influxDBClient;
        }
        influxDBClient.enableGzip();

        return influxDBClient;
    }
}
