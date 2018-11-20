package org.apache.pulsar.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.pulsar.grpc.proto.*;


public class PulsarGrpcService extends PulsarGrpc.PulsarImplBase {

    private final GrpcService service;

    public PulsarGrpcService(GrpcService service) {
        this.service = service;
    }

    @Override
    public StreamObserver<ProducerMessage> produce(StreamObserver<ProducerAck> ackStreamObserver) {
        ProducerHandler handler = new ProducerHandler(service, ackStreamObserver);
        return handler.produce();
    }

    @Override
    public StreamObserver<ConsumerAck> consume(StreamObserver<ConsumerMessage> messageStreamObserver) {
        ConsumerHandler handler = new ConsumerHandler(service, messageStreamObserver);
        return handler.consume();
    }

}
