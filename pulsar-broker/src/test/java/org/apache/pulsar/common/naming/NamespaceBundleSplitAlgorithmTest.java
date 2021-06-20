package org.apache.pulsar.common.naming;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_NAME;
import static org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm.TOPIC_COUNT_EQUALLY_DIVIDE;


public class NamespaceBundleSplitAlgorithmTest {

    @Test
    public void testOfMethodReturnCorrectValue() {
        NamespaceBundleSplitAlgorithm nullValue = NamespaceBundleSplitAlgorithm.of(null);
        Assert.assertNull(nullValue);
        NamespaceBundleSplitAlgorithm whatever = NamespaceBundleSplitAlgorithm.of("whatever");
        Assert.assertNull(whatever);
        NamespaceBundleSplitAlgorithm rangeEquallyDivideName = NamespaceBundleSplitAlgorithm.of(RANGE_EQUALLY_DIVIDE_NAME);
        Assert.assertTrue(rangeEquallyDivideName instanceof RangeEquallyDivideBundleSplitAlgorithm);
        NamespaceBundleSplitAlgorithm topicCountEquallyDivide = NamespaceBundleSplitAlgorithm.of(TOPIC_COUNT_EQUALLY_DIVIDE);
        Assert.assertTrue(topicCountEquallyDivide instanceof TopicCountEquallyDivideBundleSplitAlgorithm);
    }
}