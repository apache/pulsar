package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PulsarKafkaConsumerTest {

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid value: -1 for 'max.poll.records', should be value greater than 1.")
    public void testMaxPollValidation() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList("pulsar://localhost:6650"));
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "-1");
        new PulsarKafkaConsumer<>(config, null, null);
    }

}
