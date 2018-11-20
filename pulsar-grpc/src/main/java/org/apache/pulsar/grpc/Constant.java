package org.apache.pulsar.grpc;

import io.grpc.Context;

import java.util.Map;

public class Constant {
    public static final Context.Key<Map<String, String>> GRPC_PROXY_CTX_KEY = Context.key("GrpcProxy");
}
