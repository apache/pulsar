package org.apache.pulsar.broker.web;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import java.io.IOException;
import java.util.Collections;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

public class ResponseHandlerFilterTest {
    private static final String BROKER_ADDRESS_HEADER_NAME = "broker-address";
    private static final String BROKER_ADDRESS = "127.0.0.1";

    @Test
    public void testFilterAddHeaderWhenResponseWasNotCommitted() throws ServletException, IOException {
        PulsarService mockPulsarService = mock(PulsarService.class);
        BrokerInterceptor spyBrokerInterceptor = spy(BrokerInterceptor.class);
        doReturn(BROKER_ADDRESS).when(mockPulsarService).getAdvertisedAddress();
        doReturn(spyBrokerInterceptor).when(mockPulsarService).getBrokerInterceptor();
        ServiceConfiguration mockConfig = mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(mockPulsarService).getConfig();
        doReturn(Collections.emptySet()).when(mockConfig).getBrokerInterceptors();
        ResponseHandlerFilter responseHandlerFilter = new ResponseHandlerFilter(mockPulsarService);
        HttpServletRequest mockRequest = spy(HttpServletRequest.class);
        HttpServletResponse mockResponse = spy(HttpServletResponse.class);
        doReturn(false).when(mockResponse).isCommitted();
        responseHandlerFilter.doFilter(mockRequest, mockResponse, mock(FilterChain.class));
        verify(mockResponse).addHeader(BROKER_ADDRESS_HEADER_NAME, BROKER_ADDRESS);
    }

    @Test
    public void testFilterNotAddHeaderWhenResponseWasCommitted() throws ServletException, IOException {
        PulsarService mockPulsarService = mock(PulsarService.class);
        BrokerInterceptor spyBrokerInterceptor = spy(BrokerInterceptor.class);
        doReturn(BROKER_ADDRESS).when(mockPulsarService).getAdvertisedAddress();
        doReturn(spyBrokerInterceptor).when(mockPulsarService).getBrokerInterceptor();
        ServiceConfiguration mockConfig = mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(mockPulsarService).getConfig();
        doReturn(Collections.emptySet()).when(mockConfig).getBrokerInterceptors();
        ResponseHandlerFilter responseHandlerFilter = new ResponseHandlerFilter(mockPulsarService);
        HttpServletRequest mockRequest = spy(HttpServletRequest.class);
        HttpServletResponse mockResponse = spy(HttpServletResponse.class);
        doReturn(true).when(mockResponse).isCommitted();
        responseHandlerFilter.doFilter(mockRequest, mockResponse, mock(FilterChain.class));
        assertNull(mockResponse.getHeader(BROKER_ADDRESS_HEADER_NAME));
    }

    @Test
    public void testFilterChainWasInvoke() throws ServletException, IOException {
        PulsarService mockPulsarService = mock(PulsarService.class);
        BrokerInterceptor spyBrokerInterceptor = spy(BrokerInterceptor.class);
        doReturn(BROKER_ADDRESS).when(mockPulsarService).getAdvertisedAddress();
        doReturn(spyBrokerInterceptor).when(mockPulsarService).getBrokerInterceptor();
        ServiceConfiguration mockConfig = mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(mockPulsarService).getConfig();
        doReturn(Collections.emptySet()).when(mockConfig).getBrokerInterceptors();
        ResponseHandlerFilter responseHandlerFilter = new ResponseHandlerFilter(mockPulsarService);
        HttpServletRequest mockRequest = spy(HttpServletRequest.class);
        HttpServletResponse mockResponse = spy(HttpServletResponse.class);
        FilterChain spyChain = spy(FilterChain.class);
        responseHandlerFilter.doFilter(mockRequest, mockResponse, spyChain);
        verify(spyChain).doFilter(mockRequest, mockResponse);
    }

    @Test
    public void testResponseStatusIsInternalServerError() throws ServletException, IOException {
        PulsarService mockPulsarService = mock(PulsarService.class);
        BrokerInterceptor spyBrokerInterceptor = spy(BrokerInterceptor.class);
        doReturn(BROKER_ADDRESS).when(mockPulsarService).getAdvertisedAddress();
        doReturn(spyBrokerInterceptor).when(mockPulsarService).getBrokerInterceptor();
        ServiceConfiguration mockConfig = mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(mockPulsarService).getConfig();
        doReturn(Collections.emptySet()).when(mockConfig).getBrokerInterceptors();
        ResponseHandlerFilter responseHandlerFilter = new ResponseHandlerFilter(mockPulsarService);
        HttpServletRequest mockRequest = spy(HttpServletRequest.class);
        HttpServletResponse mockResponse = spy(HttpServletResponse.class);
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
                .when(mockResponse).getStatus();
        FilterChain spyChain = spy(FilterChain.class);
        HttpSession spyHttpSession = spy(HttpSession.class);
        doReturn(spyHttpSession).when(mockRequest).getSession(false);
        responseHandlerFilter.doFilter(mockRequest, mockResponse, spyChain);
        HttpSession spySession = mockRequest.getSession(false);
        verify(spySession).invalidate();
    }

    @Test
    public void testInterceptorInvoke() throws ServletException, IOException {
        PulsarService mockPulsarService = mock(PulsarService.class);
        BrokerInterceptor spyBrokerInterceptor = spy(BrokerInterceptor.class);
        doReturn(BROKER_ADDRESS).when(mockPulsarService).getAdvertisedAddress();
        doReturn(spyBrokerInterceptor).when(mockPulsarService).getBrokerInterceptor();
        ServiceConfiguration mockConfig = mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(mockPulsarService).getConfig();
        doReturn(Sets.newHashSet("test1","test2")).when(mockConfig).getBrokerInterceptors();
        ResponseHandlerFilter responseHandlerFilter = new ResponseHandlerFilter(mockPulsarService);
        HttpServletRequest mockRequest = spy(HttpServletRequest.class);
        HttpServletResponse mockResponse = spy(HttpServletResponse.class);
        responseHandlerFilter.doFilter(mockRequest, mockResponse, mock(FilterChain.class));
        verify(spyBrokerInterceptor).onWebserviceResponse(mockRequest,mockResponse);
    }

}