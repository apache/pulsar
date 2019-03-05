package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.kafka.compat.PulsarClientKafkaConfig;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class PulsarKafkaProducerTest {

    @Test
    public void testPulsarKafkaProducer() {
        ClientBuilder mockClientBuilder = mock(ClientBuilder.class);
        doReturn(mockClientBuilder).when(mockClientBuilder).serviceUrl(anyString());
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Assert.assertEquals((int)invocation.getArguments()[0], 1000, "Keep alive interval is suppose to be 1000.");
                return mockClientBuilder;
            }
        }).when(mockClientBuilder).keepAliveInterval(anyInt(), any(TimeUnit.class));

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList("pulsar://localhost:6650"));
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "1000000");

        PulsarKafkaProducer<String, String> pulsarKafkaProducer = new PulsarKafkaProducer<>(properties, null, null);
    }
}
