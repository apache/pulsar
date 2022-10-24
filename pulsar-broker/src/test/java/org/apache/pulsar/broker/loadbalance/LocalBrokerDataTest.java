package org.apache.pulsar.broker.loadbalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LocalBrokerDataTest {
    /*
    Ensuming that there no bundleStats field in the json string serialized from LocalBrokerData.
     */
    @Test
    public void testSerializeLocalBrokerData() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        LocalBrokerData localBrokerData = new LocalBrokerData();
        assert !objectMapper.writeValueAsString(localBrokerData).contains("bundleStats");
    }
}
