package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.*;

import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.protocols.grpc.Constants.*;

public class GrpcServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context ctx = Context.current();

        if (metadata.containsKey(PRODUCER_PARAMS_METADATA_KEY)) {
            try {
                PulsarApi.CommandProducer params = PulsarApi.CommandProducer.parseFrom(metadata.get(PRODUCER_PARAMS_METADATA_KEY));
                checkArgument(!params.getTopic().isEmpty(), "Empty topic name");
                ctx = ctx.withValue(PRODUCER_PARAMS_CTX_KEY, params);
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                throw Status.INVALID_ARGUMENT.withDescription("Invalid producer metadata: " + e.getMessage()).asRuntimeException(metadata);
            }
        }

        SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        ctx = ctx.withValue(REMOTE_ADDRESS_CTX_KEY, socketAddress);

        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }

}
