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
import org.testcontainers.containers.GenericContainer;
import org.testng.collections.Maps;

/**
 * A tester used for testing a specific sink.
 */
public abstract class SinkTester<SINK_SERVICE_CONTAINER extends GenericContainer> {

    protected final String sinkType;
    protected final Map<String, Object> sinkConfig;

    protected SinkTester(String sinkType) {
        this.sinkType = sinkType;
        this.sinkConfig = Maps.newHashMap();
    }

    protected abstract SINK_SERVICE_CONTAINER newSinkService(String clusterName);

    protected String sinkType() {
        return sinkType;
    }

    protected Map<String, Object> sinkConfig() {
        return sinkConfig;
    }

    protected abstract void prepareSink();

    protected abstract void validateSinkResult(Map<String, String> kvs);

}
