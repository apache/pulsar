package org.apache.pulsar;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Test;

public class PulsarStandaloneBuilderTest {

    @Test
    public void testStandaloneBuilder() throws Exception {

        PulsarStandaloneBuilder builder = PulsarStandaloneBuilder.instance();
        
        PulsarStandalone standalone = builder.build();
        standalone.start();

        PulsarClient client = builder.buildClient();
        PulsarAdmin admin = builder.buildAdmin();

        assertNotNull(client);
        assertNotNull(admin);

        client.close();
        admin.close();
        standalone.stop();
    }

}
