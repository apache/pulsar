package org.apache.pulsar.broker.validator;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

import java.util.Optional;

public class MultipleListenerValidatorTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAppearTogether() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedAddress("127.0.0.1");
        config.setAdvertisedListeners("internal:pulsar://192.168.1.11:6660,internal:pulsar+ssl://192.168.1.11:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_1() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651,"
                + " internal:pulsar://192.168.1.11:6660, internal:pulsar+ssl://192.168.1.11:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_2() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " internal:pulsar://192.168.1.11:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDifferentListenerWithSameHostPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " external:pulsar://127.0.0.1:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerWithoutTLSPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test
    public void testListenerWithTLSPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerWithoutNonTLSAddress() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testWithoutListenerNameInAdvertisedListeners() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("external");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

}
