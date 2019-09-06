package org.apache.pulsar.broker.protocol;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.testng.annotations.Test;

/**
 * Unit test {@link ProtocolHandlerWithClassLoader}.
 */
public class ProtocolHandlerWithClassLoaderTest {

    @Test
    public void testWrapper() throws Exception {
        ProtocolHandler h = mock(ProtocolHandler.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        ProtocolHandlerWithClassLoader wrapper = new ProtocolHandlerWithClassLoader(h, loader);

        String protocol = "kafka";

        when(h.protocolName()).thenReturn(protocol);
        assertEquals(protocol, wrapper.protocolName());
        verify(h, times(1)).protocolName();

        wrapper.accept(protocol);
        verify(h, times(1)).accept(same(protocol));

        ServiceConfiguration conf = new ServiceConfiguration();
        wrapper.initialize(conf);
        verify(h, times(1)).initialize(same(conf));

        BrokerService service = mock(BrokerService.class);
        wrapper.start(service);
        verify(h, times(1)).start(service);

        when(h.)
        wrapper.getProtocolDataToAdvertise();

    }

}
