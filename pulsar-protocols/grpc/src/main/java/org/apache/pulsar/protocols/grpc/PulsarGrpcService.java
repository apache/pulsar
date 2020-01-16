package org.apache.pulsar.protocols.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;

public class PulsarGrpcService extends PulsarGrpcServiceGrpc.PulsarGrpcServiceImplBase {

    private final BrokerService brokerService;

    public PulsarGrpcService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public void hello(SimpleValue request, StreamObserver<SimpleValue> responseObserver) {
        brokerService.getTopic(request.getName(), true)
                .whenComplete((topic, throwable) ->
                        {
                            if (throwable == null) {
                                String state = topic.map(topic1 -> topic1.getInternalStats().state).orElse("null");
                                SimpleValue reply = SimpleValue.newBuilder().setName(state).build();
                                responseObserver.onNext(reply);
                                responseObserver.onCompleted();
                            } else {
                                responseObserver.onError(throwable);
                            }

                        }
                );

    }
}
