package org.apache.pulsar.proxy.server;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.servlet.ServletException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AdminProxyHandler extends AsyncProxyServlet.Transparent {
    private static final Logger LOG = LoggerFactory.getLogger(AdminProxyHandler.class);

    private final ProxyConfiguration config;

    AdminProxyHandler(ProxyConfiguration config) {
        this.config = config;
    }

    @Override
    protected HttpClient createHttpClient() throws ServletException {
        HttpClient client = super.createHttpClient();
        client.setFollowRedirects(true);
        return client;
    }

    @Override
    protected HttpClient newHttpClient() {
        try {
            Authentication auth = AuthenticationFactory.create(
                config.getBrokerClientAuthenticationPlugin(),
                config.getBrokerClientAuthenticationParameters()
            );

            Objects.requireNonNull(auth, "No supported auth found for proxy");

            auth.start();

            boolean useTls = config.getBrokerServiceURL().startsWith("https://");

            if (useTls) {
                try {
                    X509Certificate trustCertificates[] = SecurityUtility
                        .loadCertificatesFromPemFile(config.getTlsTrustCertsFilePath());

                    SSLContext sslCtx;
                    AuthenticationDataProvider authData = auth.getAuthData();
                    if (authData.hasDataForTls()) {
                        sslCtx = SecurityUtility.createSslContext(
                            config.isTlsAllowInsecureConnection(),
                            trustCertificates,
                            authData.getTlsCertificates(),
                            authData.getTlsPrivateKey()
                        );
                    } else {
                        sslCtx = SecurityUtility.createSslContext(
                            config.isTlsAllowInsecureConnection(),
                            trustCertificates
                        );
                    }

                    SslContextFactory contextFactory = new SslContextFactory();
                    contextFactory.setSslContext(sslCtx);

                    return new HttpClient(contextFactory);
                } catch (Exception e) {
                    try {
                        auth.close();
                    } catch (IOException ioe) {
                        LOG.error("Failed to close the authentication service", ioe);
                    }
                    throw new PulsarClientException.InvalidConfigurationException(e.getMessage());
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        // return an unauthenticated client, every request will fail.
        return new HttpClient();
    }
}
