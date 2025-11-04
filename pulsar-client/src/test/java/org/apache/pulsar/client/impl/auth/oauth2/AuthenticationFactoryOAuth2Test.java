package org.apache.pulsar.client.impl.auth.oauth2;

import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.net.URL;
import org.apache.pulsar.client.api.Authentication;
import org.testng.annotations.Test;

public class AuthenticationFactoryOAuth2Test {

    @Test
    public void testBuilder() throws IOException {
        URL issuerUrl = new URL("http://localhost");
        URL credentialsUrl = new URL("http://localhost");
        String audience = "audience";
        String scope = "scope";
        Integer connectTimeout = 10001;
        Integer readTimeout = 30001;
        String trustCertsFilePath = null;
        try (Authentication authentication =
                     AuthenticationFactoryOAuth2.clientCredentialsBuilder().issuerUrl(issuerUrl)
                             .credentialsUrl(credentialsUrl).audience(audience).scope(scope)
                             .connectTimeout(connectTimeout).readTimeout(readTimeout)
                             .trustCertsFilePath(trustCertsFilePath).build()) {
            assertTrue(authentication instanceof AuthenticationOAuth2);
        }
    }

    @Test
    public void testClientCredentials() throws IOException {
        URL issuerUrl = new URL("http://localhost");
        URL credentialsUrl = new URL("http://localhost");
        String audience = "audience";
        try (Authentication authentication =
                     AuthenticationFactoryOAuth2.clientCredentials(issuerUrl, credentialsUrl, audience)) {
            assertTrue(authentication instanceof AuthenticationOAuth2);
        }
    }

}
