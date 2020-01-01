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
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.schema.KeyValue;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.collections.Maps;

/**
 * A tester used for testing a specific source.
 */
@Getter
@Slf4j
public abstract class SourceTester<ServiceContainerT extends GenericContainer> {

    public final static String INSERT = "INSERT";

    public final static String DELETE = "DELETE";

    public final static String UPDATE = "UPDATE";

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

    public abstract void prepareInsertEvent() throws Exception;

    public abstract void prepareDeleteEvent() throws Exception;

    public abstract void prepareUpdateEvent() throws Exception;

    public abstract Map<String, String> produceSourceMessages(int numMessages) throws Exception;

    public void validateSourceResult(Consumer<KeyValue<byte[], byte[]>> consumer, int number, String eventType) throws Exception {
        int recordsNumber = 0;
        Message<KeyValue<byte[], byte[]>> msg = consumer.receive(2, TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            final String key = new String(msg.getValue().getKey());
            final String value = new String(msg.getValue().getValue());
            log.info("Received message: key = {}, value = {}.", key, value);
            Assert.assertTrue(key.contains(this.keyContains()));
            Assert.assertTrue(value.contains(this.valueContains()));
            if (eventType != null) {
                Assert.assertTrue(value.contains(this.eventContains(eventType)));
            }
            consumer.acknowledge(msg);
            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, number);
        log.info("Stop {} server container. topic: {} has {} records.", getSourceType(), consumer.getTopic(), recordsNumber);
    }
    public String keyContains(){
        return "dbserver1.inventory.products.Key";
    }
    public String valueContains(){
        return "dbserver1.inventory.products.Value";
    }

    public String eventContains(String eventType) {
        if (eventType.equals(INSERT)) {
            return "\"op\":\"c\"";
        } else if (eventType.equals(UPDATE)) {
            return "\"op\":\"u\"";
        } else {
            return "\"op\":\"d\"";
        }
    }
}
