package org.apache.pulsar.protocols.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;

import java.net.SocketAddress;

import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;

public class Constants {
    public static final Metadata.Key<byte[]> PRODUCER_PARAMS_METADATA_KEY = Metadata.Key.of("pulsar-producer-params-bin", BINARY_BYTE_MARSHALLER);

    public static final Context.Key<CommandProducer> PRODUCER_PARAMS_CTX_KEY = Context.key("ProducerParams");
    public static final Context.Key<SocketAddress> REMOTE_ADDRESS_CTX_KEY = Context.key("RemoteAddress");

}
