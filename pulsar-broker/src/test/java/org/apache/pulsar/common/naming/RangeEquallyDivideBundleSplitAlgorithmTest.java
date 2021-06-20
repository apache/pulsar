package org.apache.pulsar.common.naming;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.concurrent.CompletableFuture;

public class RangeEquallyDivideBundleSplitAlgorithmTest {

    @Test
    public void testGetSplitBoundaryMethodReturnCorrectResult() {
        RangeEquallyDivideBundleSplitAlgorithm rangeEquallyDivideBundleSplitAlgorithm = new RangeEquallyDivideBundleSplitAlgorithm();
        Assert.assertThrows(NullPointerException.class, () -> rangeEquallyDivideBundleSplitAlgorithm.getSplitBoundary(null, null));
        long lowerRange = 10L;
        long upperRange = 0xffffffffL;
        long correctResult = lowerRange + (upperRange - lowerRange) / 2;
        NamespaceBundle namespaceBundle = new NamespaceBundle(NamespaceName.SYSTEM_NAMESPACE, Range.range(lowerRange, BoundType.CLOSED, upperRange, BoundType.CLOSED),
                Mockito.mock(NamespaceBundleFactory.class));
        CompletableFuture<Long> splitBoundary = rangeEquallyDivideBundleSplitAlgorithm.getSplitBoundary(null, namespaceBundle);
        Long value = splitBoundary.join();
        Assert.assertEquals((long) value, correctResult);
    }
}