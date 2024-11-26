package org.apache.pulsar.broker.authentication;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;

public class AuthenticationDataSubscriptionTest {

    AuthenticationDataSubscription target;

    @Test
    public void testTargetFromAuthenticationDataHttp(){
        var req = mock(HttpServletRequest.class);
        String headerName = "Authorization";
        String headerValue = "my-header";
        String authType = "my-authType";
        doReturn(headerValue).when(req).getHeader(eq(headerName));
        doReturn("localhost").when(req).getRemoteAddr();
        doReturn(4000).when(req).getRemotePort();
        doReturn(authType).when(req).getAuthType();
        AuthenticationDataSource authenticationDataSource = new AuthenticationDataHttp(req);
        target = new AuthenticationDataSubscription(authenticationDataSource, "my-sub");
        assertEquals(headerValue, target.getHttpHeader(headerName));
        assertEquals(authType, target.getHttpAuthType());
        assertEquals(true, target.hasDataFromHttp());
    }
}
