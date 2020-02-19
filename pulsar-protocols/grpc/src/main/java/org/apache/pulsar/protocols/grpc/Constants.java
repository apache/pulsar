package org.apache.pulsar.protocols.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;

import javax.net.ssl.SSLSession;
import java.net.SocketAddress;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;

public class Constants {
    public static final Metadata.Key<byte[]> PRODUCER_PARAMS_METADATA_KEY = Metadata.Key.of("pulsar-producer-params-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<String> ERROR_CODE_METADATA_KEY = Metadata.Key.of("pulsar-error-code", ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTH_METADATA_KEY = Metadata.Key.of("pulsar-auth-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTHCHALLENGE_METADATA_KEY = Metadata.Key.of("pulsar-authchallenge-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTHRESPONSE_METADATA_KEY = Metadata.Key.of("pulsar-authresponse-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTH_ROLE_TOKEN_METADATA_KEY = Metadata.Key.of("pulsar-authroletoken-bin", BINARY_BYTE_MARSHALLER);

    public static final Context.Key<CommandProducer> PRODUCER_PARAMS_CTX_KEY = Context.key("ProducerParams");
    public static final Context.Key<SocketAddress> REMOTE_ADDRESS_CTX_KEY = Context.key("RemoteAddress");
    public static final Context.Key<SSLSession> SSL_SESSION_CTX_KEY = Context.key("SSLSession");
    public static final Context.Key<String> AUTH_ROLE_CTX_KEY = Context.key("AuthRole");
    public static final Context.Key<AuthenticationDataSource> AUTH_DATA_CTX_KEY = Context.key("AuthenticationData");

}
