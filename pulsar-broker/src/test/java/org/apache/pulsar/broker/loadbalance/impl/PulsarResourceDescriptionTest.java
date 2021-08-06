package org.apache.pulsar.broker.loadbalance.impl;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarResourceDescriptionTest {

    @Test
    public void compareTo() {
        PulsarResourceDescription one = new PulsarResourceDescription();
        one.put("cpu", new ResourceUsage(0.1, 0.2));
        PulsarResourceDescription two = new PulsarResourceDescription();
        two.put("cpu", new ResourceUsage(0.1, 0.2));
        assertEquals(0, one.compareTo(two));
    }
}