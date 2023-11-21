package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AsyncTokenBucketTest {
    private AtomicLong manualClockSource;
    private LongSupplier clockSource;

    private AsyncTokenBucket asyncTokenBucket;

    @BeforeMethod
    public void setup() {
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        clockSource = manualClockSource::get;
    }


    private void incrementSeconds(int seconds) {
        manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
        asyncTokenBucket.updateTokens();
    }

    private void incrementMillis(long millis) {
        manualClockSource.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    @Test
    void shouldAddTokensWithConfiguredRate() {
        asyncTokenBucket = new AsyncTokenBucket(100, 10, clockSource);
        incrementSeconds(5);
        assertEquals(50, asyncTokenBucket.tokens(true));
        incrementSeconds(1);
        assertEquals(60, asyncTokenBucket.tokens(true));
        incrementSeconds(10);
        assertEquals(100, asyncTokenBucket.tokens(true));
    }

    @Test
    void shouldCalculatePauseCorrectly() {
        asyncTokenBucket = new AsyncTokenBucket(100, 10, clockSource);
        incrementSeconds(5);
        asyncTokenBucket.consumeTokens(100);
        assertEquals(-50, asyncTokenBucket.tokens(true));
        assertEquals(6, TimeUnit.NANOSECONDS.toSeconds(asyncTokenBucket.calculatePauseNanos(10, true)));
    }

    @Test
    void shouldSupportFractionsWhenUpdatingTokens() {
        asyncTokenBucket = new AsyncTokenBucket(100, 10, clockSource);
        incrementMillis(100);
        assertEquals(1, asyncTokenBucket.tokens(true));
    }

    @Test
    void shouldSupportFractionsAndRetainLeftoverWhenUpdatingTokens() {
        asyncTokenBucket = new AsyncTokenBucket(100, 10, clockSource);
        for (int i = 0; i < 150; i++) {
            incrementMillis(1);
        }
        assertEquals(1, asyncTokenBucket.tokens(true));
        incrementMillis(150);
        assertEquals(3, asyncTokenBucket.tokens(true));
    }

}