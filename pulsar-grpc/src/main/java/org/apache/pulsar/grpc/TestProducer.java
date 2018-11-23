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
package org.apache.pulsar.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.grpc.proto.ClientParameters;
import org.apache.pulsar.grpc.proto.ProducerAck;
import org.apache.pulsar.grpc.proto.ProducerMessage;
import org.apache.pulsar.grpc.proto.PulsarGrpc;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.pulsar.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;

public class TestProducer {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        Metadata headers = new Metadata();
        byte[] params = ClientParameters.newBuilder().setTopic("topic").build().toByteArray();
        headers.put(CLIENT_PARAMS_METADATA_KEY, params);
        PulsarGrpc.PulsarStub asyncStub = MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel), headers);

        StreamObserver<ProducerMessage> producer = asyncStub.produce(new StreamObserver<ProducerAck>() {
            @Override
            public void onNext(ProducerAck value) {
                String msgId = value.getMessageId().toStringUtf8();
                System.out.println("producer ack received: " + value.getContext() + " " + value.getStatusCode() + " "+ Instant.now());
                LoggerFactory.getLogger("foo").error("" + value.getStatusCode());
            }

            @Override
            public void onError(Throwable t) {
                LoggerFactory.getLogger("foo").error(t.getMessage());
            }

            @Override
            public void onCompleted() {
                LoggerFactory.getLogger("foo").error("completed");
            }
        });

        for(int i=0; i < 10; i++) {
            producer.onNext(ProducerMessage.newBuilder()
                    .setPayload(ByteString.copyFromUtf8("test" + i))
                    .setContext("" + i)
                    .build());
        }

        while(true) {
            Thread.sleep(1);
        }
    }
}
