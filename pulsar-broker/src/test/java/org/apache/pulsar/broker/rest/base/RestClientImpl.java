package org.apache.pulsar.broker.rest.base;


import org.apache.pulsar.broker.rest.base.api.RestClient;
import org.apache.pulsar.broker.rest.base.api.RestConsumer;
import org.apache.pulsar.broker.rest.base.api.RestProducer;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnectorProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.net.ServiceURI;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.concurrent.TimeUnit;

public class RestClientImpl implements RestClient {

    public static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_READ_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 300;
    public static final int DEFAULT_CERT_REFRESH_SECONDS = 300;

    private final int readTimeout;
    private final String serviceUrl;
    private final WebTarget root;
    private final Authentication auth;
    private final Client client;

    private final RestProducer producer;
    private final RestConsumer consumer;

    public RestClientImpl(String serviceUrl, ClientConfigurationData clientConfigData) throws PulsarClientException {
        this(serviceUrl, clientConfigData, DEFAULT_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS,
                DEFAULT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS, DEFAULT_CERT_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    public RestClientImpl(String serviceUrl,
                          ClientConfigurationData clientConfigData,
                          int connectTimeout,
                          TimeUnit connectTimeoutUnit,
                          int readTimeout,
                          TimeUnit readTimeoutUnit,
                          int autoCertRefreshTime,
                          TimeUnit autoCertRefreshTimeUnit) throws PulsarClientException {

        this.readTimeout = readTimeout;

        this.auth = clientConfigData != null ? clientConfigData.getAuthentication() : new AuthenticationDisabled();

        if (auth != null) {
            auth.start();
        }

        AsyncHttpConnectorProvider asyncConnectorProvider = new AsyncHttpConnectorProvider(clientConfigData,
                (int) autoCertRefreshTimeUnit.toSeconds(autoCertRefreshTime));

        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);
        httpConfig.connectorProvider(asyncConnectorProvider);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder()
                .withConfig(httpConfig)
                .connectTimeout(connectTimeout, connectTimeoutUnit)
                .readTimeout(readTimeout, readTimeoutUnit)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        this.client = clientBuilder.build();

        this.serviceUrl = serviceUrl;
        ServiceURI serviceUri = ServiceURI.create(serviceUrl);
        this.root = client.target(serviceUri.selectOne());

        long readTimeoutMs = readTimeoutUnit.toMillis(this.readTimeout);
        this.producer = new RestProducerImpl(root, auth, readTimeoutMs);
        this.consumer = new RestConsumerImpl(root, auth, readTimeoutMs);
    }

    @Override
    public void close() {

    }

    @Override
    public RestProducer producer() {
        return producer;
    }

    @Override
    public RestConsumer consumer() {
        return consumer;
    }
}
