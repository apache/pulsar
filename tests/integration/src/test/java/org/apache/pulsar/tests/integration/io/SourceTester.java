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
 * A tester used for testing a specific source.
 */
@Getter
public abstract class SourceTester<ServiceContainerT extends GenericContainer> {

    protected final String sourceType;
    protected final Map<String, Object> sourceConfig;

    protected SourceTester(String sourceType) {
        this.sourceType = sourceType;
        this.sourceConfig = Maps.newHashMap();
    }

    public abstract void setServiceContainer(ServiceContainerT serviceContainer);

    public String sourceType() {
        return sourceType;
    }

    public Map<String, Object> sourceConfig() {
        return sourceConfig;
    }

    public abstract void prepareSource() throws Exception;

    public abstract Map<String, String> produceSourceMessages(int numMessages) throws Exception;

}
