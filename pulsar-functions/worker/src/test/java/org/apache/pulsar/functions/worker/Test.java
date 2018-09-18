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
package org.apache.pulsar.functions.worker;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.functions.proto.Request;

import java.io.IOException;

@Slf4j
public class Test {

    @org.testng.annotations.Test
    public void test() throws IOException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();


        Reader<byte[]> reader = pulsarClient.newReader()
                .topic("persistent://public/functions/assignments")
                .startMessageId(MessageId.earliest)
                .create();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> msg = reader.readNext();

            Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(msg.getData());

            log.info("assignmentsUpdate: {}", assignmentsUpdate);
        }

        reader.close();
        pulsarClient.close();
    }

}
