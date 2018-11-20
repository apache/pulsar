package org.apache.pulsar.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.grpc.proto.ConsumerAck;
import org.apache.pulsar.grpc.proto.ConsumerMessage;
import org.apache.pulsar.grpc.proto.PulsarGrpc;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class TestClient {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("pulsar-topic", ASCII_STRING_MARSHALLER), "persistent://public/default/my-topic12");
        headers.put(Metadata.Key.of("pulsar-subscription", ASCII_STRING_MARSHALLER), "my-subscription");
        headers.put(Metadata.Key.of("pulsar-subscription-type", ASCII_STRING_MARSHALLER), "Shared");
        headers.put(Metadata.Key.of("pulsar-max-redeliver-count", ASCII_STRING_MARSHALLER), "3");
        headers.put(Metadata.Key.of("pulsar-ack-timeout-millis", ASCII_STRING_MARSHALLER), "2000");
        headers.put(Metadata.Key.of("pulsar-consumer-name", ASCII_STRING_MARSHALLER), "test");

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
            if(msgId != null) {
                /*ackStreamObserver.onNext(ConsumerAck.newBuilder()
                        .setMessageId(ByteString.copyFrom(msgId))
                        .build());*/
            }
            Thread.sleep(1);
        }
    }
}
