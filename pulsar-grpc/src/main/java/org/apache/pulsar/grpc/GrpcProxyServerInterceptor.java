package org.apache.pulsar.grpc;

import io.grpc.*;

import java.util.Map;
import java.util.stream.Collectors;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.apache.pulsar.grpc.Constant.GRPC_PROXY_CTX_KEY;

public class GrpcProxyServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Map<String, String> params = metadata.keys().stream()
                .filter(key -> key.startsWith("pulsar-"))
                .collect(Collectors.toMap(k -> k, k -> metadata.get(Metadata.Key.of(k, ASCII_STRING_MARSHALLER))));

        Context ctx = Context.current().withValue(GRPC_PROXY_CTX_KEY, params);
        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }

}
