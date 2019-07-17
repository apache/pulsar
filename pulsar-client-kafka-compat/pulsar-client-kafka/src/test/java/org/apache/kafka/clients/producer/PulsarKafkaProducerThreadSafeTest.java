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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;
import java.util.Properties;
/**
 * A test that tests if {@link PulsarKafkaProducer} is thread safe.
 */
public class PulsarKafkaProducerThreadSafeTest {
    private static Properties props = new Properties();
    static {
        props.put("bootstrap.servers", "pulsar://localhost:6650");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
    }
    private static PulsarKafkaProducer producer = new PulsarKafkaProducer<>(props, new StringSerializer(), new StringSerializer());

   /**
    * This test run 10 times in threadPool witch size is 5.
    * Different threads have same producer and different topics witch is based on thread time.
    * This test will be failed when producer failed to send if PulsarKafkaProducer is not thread safe.
    */
    @Test(threadPoolSize = 5, invocationCount = 10)
    public static void testPulsarKafkaProducerThreadSafe(){
        String topic1 = "persistent://public/default/topic-" + System.currentTimeMillis();
        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic1, "Hello");
        producer.send(record1, (recordMetadata, e) -> {
            throw new Error();
        });
    }
}
