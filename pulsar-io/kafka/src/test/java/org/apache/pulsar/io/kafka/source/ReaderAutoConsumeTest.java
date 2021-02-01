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
package org.apache.pulsar.io.kafka.source;/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author eolivelli
 */
public class ReaderAutoConsumeTest {

    public ReaderAutoConsumeTest() {
    }

    @Test
    public void hello() throws Exception {
        try (PulsarClient client = PulsarClient
                .builder()
                .serviceUrl("http://localhost:8080")
                .build()) {
            try (Reader<byte[]> p = client
                    .newReader(Schema.BYTES)
                    .topic("persistent://public/default/fromk14")
                    .startMessageId(MessageId.earliest)
                    .startMessageIdInclusive()
                    .create()) {
                System.out.println("qui: " + p);
                while (true) {
                    Message<?> msg = p.readNext(10, TimeUnit.SECONDS);
                    if (msg != null) {
                        byte[] schemaVersion = msg.getSchemaVersion() != null ? msg.getSchemaVersion() : new byte[0];
                        //System.out.println("Message: " + msg.getKey() + " " + msg.getValue() + " " + Arrays.toString(schemaVersion)+" "+msg.getValue().getFields());
                        System.out.println("Message: " + msg.getKey() + " " + msg.getValue() + " " + Arrays.toString(schemaVersion));
                        /*for (Field name : msg.getValue().getFields()) {
                            System.out.println("field "+name.getName()+" alue "+msg.getValue().getField(name));
                        }*/
                    } else {
                        System.out.println("nothing");
                    }

                }
            }
        }
    }

}
