package org.apache.pulsar.logger;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import org.testng.annotations.Test;

public class VerboseLoggerThrottleTest {

    @Test
    public void testAcquire() throws Exception {
        long periodInSecond = 5;
        int permits = 10;
        VerboseLoggerThrottle throttle = new VerboseLoggerThrottle(periodInSecond, permits);
        for (int i = 0; i < permits; i++) {
            assertTrue(throttle.acquire());
        }
        assertFalse(throttle.acquire());
        Thread.sleep(periodInSecond * 1000);
        for (int i = 0; i < permits; i++) {
            assertTrue(throttle.acquire());
        }
    }
}
