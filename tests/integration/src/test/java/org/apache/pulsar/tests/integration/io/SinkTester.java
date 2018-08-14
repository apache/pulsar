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
import org.testcontainers.containers.GenericContainer;
import org.testng.collections.Maps;

/**
 * A tester used for testing a specific sink.
 */
@Getter
public abstract class SinkTester {

    protected final String sinkType;
    protected final Map<String, Object> sinkConfig;

    public SinkTester(String sinkType) {
        this.sinkType = sinkType;
        this.sinkConfig = Maps.newHashMap();
    }

    public abstract void findSinkServiceContainer(Map<String, GenericContainer<?>> externalServices);

    public String sinkType() {
        return sinkType;
    }

    public Map<String, Object> sinkConfig() {
        return sinkConfig;
    }

    public abstract void prepareSink() throws Exception;

    public abstract void validateSinkResult(Map<String, String> kvs);

}
