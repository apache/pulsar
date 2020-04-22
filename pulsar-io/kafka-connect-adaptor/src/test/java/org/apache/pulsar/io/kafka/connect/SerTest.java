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
package org.apache.pulsar.io.kafka.connect;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Slf4j
public class SerTest {

    @Test
    public void test() {

        Schema schema = KeyValueSchema.of(new KafkaSchemaWrappedSchema(null, null),
                new KafkaSchemaWrappedSchema(null, null), KeyValueEncodingType.SEPARATED);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);
            oos.writeObject(schema);
            oos.flush();
            oos.close();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);
            Schema schema2 = (Schema) ois.readObject();
            log.info("deserializable schema: {}, classLoader: {}",
                    schema.getClass().getName(), schema.getClass().getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
