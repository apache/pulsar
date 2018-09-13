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
package org.apache.pulsar.tests.integration.io;

import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.client.api.Schema;
import org.testcontainers.containers.GenericContainer;
import org.testng.collections.Maps;

/**
 * A tester used for testing a specific sink.
 */
@Getter
public abstract class SinkTester {

    public enum SinkType {
        UNDEFINED,
        CASSANDRA,
        KAFKA,
        JDBC,
        HDFS,
        ELASTIC_SEARCH
    }

    protected final SinkType sinkType;
    protected final String sinkArchive;
    protected final String sinkClassName;
    protected final Map<String, Object> sinkConfig;

    public SinkTester(SinkType sinkType) {
        this.sinkType = sinkType;
        this.sinkArchive = null;
        this.sinkClassName = null;
        this.sinkConfig = Maps.newHashMap();
    }

    public SinkTester(String sinkArchive, String sinkClassName) {
        this.sinkType = SinkType.UNDEFINED;
        this.sinkArchive = sinkArchive;
        this.sinkClassName = sinkClassName;
        this.sinkConfig = Maps.newHashMap();
    }

    public Schema<?> getInputTopicSchema() {
        return Schema.STRING;
    }

    public abstract void findSinkServiceContainer(Map<String, GenericContainer<?>> externalServices);

    public SinkType sinkType() {
        return sinkType;
    }

    public Map<String, Object> sinkConfig() {
        return sinkConfig;
    }

    public abstract void prepareSink() throws Exception;

    public abstract void validateSinkResult(Map<String, String> kvs);

}
