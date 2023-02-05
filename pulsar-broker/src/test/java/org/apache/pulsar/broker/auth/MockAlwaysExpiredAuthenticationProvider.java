package org.apache.pulsar.broker.auth;

import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;

/**
 * Class that provides the same authentication semantics as the {@link MockAuthenticationProvider} except
 * that this one initializes the {@link MockAlwaysExpiredAuthenticationState} class to support testing
 * expired authentication and auth refresh.
 */
public class MockAlwaysExpiredAuthenticationProvider extends MockAuthenticationProvider {

    @Override
    public String getAuthMethodName() {
        return "always-expired";
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData,
                                            SocketAddress remoteAddress,
                                            SSLSession sslSession) throws AuthenticationException {
        return new MockAlwaysExpiredAuthenticationState(this);
    }
}
