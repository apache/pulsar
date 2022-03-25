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
package org.apache.pulsar.io.influxdb.v1;

import com.google.common.base.Strings;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

@Slf4j
public class InfluxDBBuilderImpl implements InfluxDBBuilder {

    @Override
    public InfluxDB build(InfluxDBSinkConfig config) {
        InfluxDB influxDB;

        boolean enableAuth = !Strings.isNullOrEmpty(config.getUsername());
        if (enableAuth) {
            log.info("Authenticating to {} as {}", config.getInfluxdbUrl(), config.getUsername());
            influxDB = InfluxDBFactory.connect(config.getInfluxdbUrl(), config.getUsername(), config.getPassword());
        } else {
            log.info("Connecting to {}", config.getInfluxdbUrl());
            influxDB = InfluxDBFactory.connect(config.getInfluxdbUrl());
        }

        if (config.isGzipEnable()) {
            influxDB.enableGzip();
        }

        InfluxDB.LogLevel logLevel;
        try {
            logLevel = InfluxDB.LogLevel.valueOf(config.getLogLevel().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal Log Level, valid values are: "
                + Arrays.asList(InfluxDB.LogLevel.values()));
        }
        influxDB.setLogLevel(logLevel);

        return influxDB;
    }
}
