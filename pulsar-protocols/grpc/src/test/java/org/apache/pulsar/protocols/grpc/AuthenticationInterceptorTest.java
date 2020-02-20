package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.services.HealthStatusManager;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderBasic;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.impl.auth.AuthenticationDataBasic;
import org.apache.pulsar.protocols.grpc.api.CommandAuth;
import org.apache.pulsar.protocols.grpc.api.CommandConnect;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.protocols.grpc.test.AuthGrpc;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.protocols.grpc.Constants.AUTH_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.AUTH_ROLE_CTX_KEY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class AuthenticationInterceptorTest {

    private static final String TEST_SERVICE = "test-service";

    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private final String BASIC_CONF_FILE_PATH = "./src/test/resources/authentication/basic/.htpasswd";

    private AuthenticationService authenticationService;

    private Server server;
    private AuthGrpc.AuthBlockingStub stub;

    @BeforeMethod
    public void setup() throws IOException {
        System.setProperty("pulsar.auth.basic.conf", BASIC_CONF_FILE_PATH);

        authenticationService = mock(AuthenticationService.class);

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(ServerInterceptors.intercept(
                new AuthGrpcService(),
                Collections.singletonList(new AuthenticationInterceptor(authenticationService))
            ))
            .build();

        server.start();

        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = AuthGrpc.newBlockingStub(channel);
    }

    @Test
    public void testAuthenticationBasic() throws IOException {
        AuthenticationProviderBasic provider = new AuthenticationProviderBasic();
        provider.initialize(null);
        doReturn(provider).when(authenticationService).getAuthenticationProvider(provider.getAuthMethodName());

        AuthenticationDataBasic clientData = new AuthenticationDataBasic("superUser", "supepass");
        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder()
            .setAuthMethod(provider.getAuthMethodName())
            .setAuthData(ByteString.copyFromUtf8(clientData.getCommandData()))
            .build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub basicStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(basicStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "superUser");
    }

    private static class AuthGrpcService extends AuthGrpc.AuthImplBase {

        @Override
        public void getPrincipal(Empty request, StreamObserver<StringValue> responseObserver) {
            String role = AUTH_ROLE_CTX_KEY.get();
            StringValue value = StringValue.newBuilder().setValue(role != null ? role : "unauthenticated user").build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        }
    }

}
