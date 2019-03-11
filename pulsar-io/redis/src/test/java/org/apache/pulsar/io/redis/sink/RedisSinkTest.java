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
package org.apache.pulsar.io.redis.sink;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.redis.EmbeddedRedisUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * redis Sink test
 */
@Slf4j
public class RedisSinkTest {

    private EmbeddedRedisUtils embeddedRedisUtils;

    /**
     * A Simple class to test redis class
     */
    @Data
    @ToString
    @EqualsAndHashCode
    public static class Foo {
        private String field1;
        private String field2;
    }

    @BeforeMethod
    public void setUp() throws Exception {
        embeddedRedisUtils = new EmbeddedRedisUtils(getClass().getSimpleName());
        embeddedRedisUtils.setUp();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        embeddedRedisUtils.tearDown();
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("redisHosts", "localhost:6379");
        configs.put("redisPassword", "");
        configs.put("redisDatabase", "1");
        configs.put("clientMode", "Standalone");
        configs.put("operationTimeout", "3000");
        configs.put("batchSize", "10");

        RedisSink sink = new RedisSink();

        // prepare a foo Record
        Foo obj = new Foo();
        obj.setField1("FakeFiled1");
        obj.setField2("FakeFiled1");
        AvroSchema<Foo> schema = AvroSchema.of(Foo.class);

        byte[] bytes = schema.encode(obj);
        ByteBuf payload = Unpooled.copiedBuffer(bytes);
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(GenericSchemaImpl.of(schema.getSchemaInfo()));

        Message<GenericRecord> message = new MessageImpl("fake_topic_name", "77:777", configs, payload, autoConsumeSchema);
        Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
            .message(message)
            .topicName("fake_topic_name")
            .build();

        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
            obj.toString(),
            message.getValue().toString(),
            record.getValue().toString());

        // open should success
        sink.open(configs, null);

        // write should success.
        sink.write(record);
        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);

    }
}
