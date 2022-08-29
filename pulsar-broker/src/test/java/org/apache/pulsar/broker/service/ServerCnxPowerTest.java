package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;


@PrepareForTest({CommandAuthResponse.class, org.apache.pulsar.common.api.proto.AuthData.class})
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*", "org.w3c.dom.*"})
public class ServerCnxPowerTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testHandleAuthResponseWithoutClientVersion() {
        ServerCnx cnx = PowerMockito.mock(ServerCnx.class, CALLS_REAL_METHODS);
        CommandAuthResponse authResponse = PowerMockito.mock(CommandAuthResponse.class);
        org.apache.pulsar.common.api.proto.AuthData authData =
                PowerMockito.mock(org.apache.pulsar.common.api.proto.AuthData.class);
        when(authResponse.getResponse()).thenReturn(authData);
        when(authResponse.hasResponse()).thenReturn(true);
        when(authResponse.getResponse().hasAuthMethodName()).thenReturn(true);
        when(authResponse.getResponse().hasAuthData()).thenReturn(true);
        when(authResponse.hasClientVersion()).thenReturn(false);
        try {
            cnx.handleAuthResponse(authResponse);
        } catch (Exception ignore) {
        }
        verify(authResponse, times(1)).hasClientVersion();
        verify(authResponse, times(0)).getClientVersion();
    }
}
