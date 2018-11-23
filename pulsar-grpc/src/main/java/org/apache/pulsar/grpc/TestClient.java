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

import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.grpc.proto.*;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.pulsar.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;
import static org.apache.pulsar.grpc.proto.ConsumerParameters.SubscriptionType.SUBSCRIPTION_TYPE_SHARED;

public class TestClient {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        Metadata headers = new Metadata();
        ClientParameters params = ClientParameters.newBuilder()
                .setTopic("topic")
                .setConsumerParameters(
                        ConsumerParameters.newBuilder()
                                .setSubscription("my-subscription")
                                .setAckTimeoutMillis(uint64Value(2000))
                                .setSubscriptionType(SUBSCRIPTION_TYPE_SHARED)
                                .setDeadLetterPolicy(DeadLetterPolicy.newBuilder()
                                        .setMaxRedeliverCount(uint32Value(3))
                                )
                )
                .build();
        headers.put(CLIENT_PARAMS_METADATA_KEY, params.toByteArray());
        PulsarGrpc.PulsarStub asyncStub = MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel), headers);

        LinkedBlockingQueue<byte[]> messagesToack = new LinkedBlockingQueue<>(1000);

        StreamObserver<ConsumerMessage> messageStreamObserver = new StreamObserver<ConsumerMessage>() {
            @Override
            public void onNext(ConsumerMessage value) {
                byte[] msgId = value.getMessageId().toByteArray();
                try {
                    messagesToack.put(msgId);
                } catch (InterruptedException e) {
                    onError(e);
                }
                String payload = value.getPayload().toStringUtf8();
                System.out.println("consumer received: " + payload + " ");
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("consumer message error: " + t.getMessage());
                LoggerFactory.getLogger("foo").error(t.getMessage());
            }

            @Override
            public void onCompleted() {
            }
        };
        StreamObserver<ConsumerAck> ackStreamObserver = asyncStub.consume(messageStreamObserver);

        while(true) {
            byte[] msgId = messagesToack.poll();
            if (msgId != null) {
                /*ackStreamObserver.onNext(ConsumerAck.newBuilder()
                        .setMessageId(ByteString.copyFrom(msgId))
                        .build());*/
            }
            Thread.sleep(1);
        }
    }

    public static UInt64Value uint64Value(long value) {
        return UInt64Value.newBuilder().setValue(value).build();
    }

    public static UInt32Value uint32Value(int value) {
        return UInt32Value.newBuilder().setValue(value).build();
    }
}
