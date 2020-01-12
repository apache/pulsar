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
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.GenericContainer;
import org.testng.collections.Maps;

/**
 * A tester used for testing a specific sink.
 */
@Getter
public abstract class SinkTester<ServiceContainerT extends GenericContainer> {

    public enum SinkType {
        UNDEFINED,
        CASSANDRA,
        KAFKA,
        JDBC,
        HDFS,
        ELASTIC_SEARCH,
        RABBITMQ
    }

    protected final String networkAlias;
    protected final SinkType sinkType;
    protected final String sinkArchive;
    protected final String sinkClassName;
    protected final Map<String, Object> sinkConfig;
    protected ServiceContainerT serviceContainer;

    public SinkTester(String networkAlias, SinkType sinkType) {
        this.networkAlias = networkAlias;
        this.sinkType = sinkType;
        this.sinkArchive = null;
        this.sinkClassName = null;
        this.sinkConfig = Maps.newHashMap();
    }

    public SinkTester(String networkAlias, String sinkArchive, String sinkClassName) {
        this.networkAlias = networkAlias;
        this.sinkType = SinkType.UNDEFINED;
        this.sinkArchive = sinkArchive;
        this.sinkClassName = sinkClassName;
        this.sinkConfig = Maps.newHashMap();
    }

    public Schema<?> getInputTopicSchema() {
        return Schema.STRING;
    }

    protected abstract ServiceContainerT createSinkService(PulsarCluster cluster);

    public ServiceContainerT startServiceContainer(PulsarCluster cluster) {
        this.serviceContainer = createSinkService(cluster);
        cluster.startService(networkAlias, serviceContainer);
        return serviceContainer;
    }

    public void stopServiceContainer(PulsarCluster cluster) {
        if (null != serviceContainer) {
            cluster.stopService(networkAlias, serviceContainer);
        }
    }

    public SinkType sinkType() {
        return sinkType;
    }

    public Map<String, Object> sinkConfig() {
        return sinkConfig;
    }

    public abstract void prepareSink() throws Exception;

    public abstract void validateSinkResult(Map<String, String> kvs);

}
