package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashRangeAutoSplitStickyKeyConsumerSelectorTest {

    @Test
    public void testGetConsumerRange() throws BrokerServiceException.ConsumerAssignException {
        HashRangeAutoSplitStickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector(2 << 5);
        List<String> consumerName = Arrays.asList("consumer1", "consumer2", "consumer3", "consumer4");
        for (int index = 0; index < consumerName.size(); index++) {
            Consumer consumer = mock(Consumer.class);
            when(consumer.consumerName()).thenReturn(consumerName.get(index));
            selector.addConsumer(consumer);
        }

        int index = 0;
        List<String> expectedConsumerName = Arrays.asList("consumer3", "consumer2", "consumer4", "consumer1");
        List<int[]> expectedRange = Arrays.asList(new int[] {0, 16}, new int[] {17, 32}, new int[] {33, 48}, new int[] {49, 64});
        for (Map.Entry<String, String> entry : selector.getConsumerRange().entrySet()) {
            Assert.assertEquals(entry.getKey(), expectedRange.get(index)[0] + "--" + expectedRange.get(index)[1]);
            Assert.assertEquals(entry.getValue(), expectedConsumerName.get(index));
            index++;
        }
    }

}
