package org.apache.pulsar.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.grpc.proto.*;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class TestProducer {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("pulsar-topic", ASCII_STRING_MARSHALLER), "persistent://public/default/my-topic12");

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
        //producer.onCompleted();

        while(true) {
            Thread.sleep(1);
        }
    }
}
