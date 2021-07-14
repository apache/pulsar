/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Implements the jitter backoff retry,
 * see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
@Slf4j
public class RandomExponentialRetry {

    /**
     * Singleton instance
     */
    public static final RandomExponentialRetry instance = new RandomExponentialRetry();

    /**
     * Maximum time in seconds between two retries.
     */
    public final long maxRetryTimeInSec;

    public RandomExponentialRetry() {
        this(TimeUnit.HOURS.toSeconds(24));
    }

    public RandomExponentialRetry(long maxRetryTimeInSec) {
        this.maxRetryTimeInSec = maxRetryTimeInSec;
    }

    public long waitInMs(int attempt, long backoffInMs) {
        assert attempt >= 0;
        assert backoffInMs >= 0;
        return Math.min(maxRetryTimeInSec * 1000, backoffInMs << attempt);
    }

    public long randomWaitInMs(int attempt, long backoffInMs) {
        return ThreadLocalRandom.current().nextLong(0, waitInMs(attempt, backoffInMs));
    }

    protected <T> T retry(Callable<T> function, int maxAttempts, long initialBackoff, String source) throws Exception {
        return retry(function, maxAttempts, initialBackoff, source, new Time());
    }

    protected <T> T retry(Callable<T> function, int maxAttempts, long initialBackoff, String source, Time clock) throws Exception {
        Exception lastException = null;
        for(int i = 0; i < maxAttempts || maxAttempts == -1; i++) {
            try {
                return function.call();
            } catch (Exception e) {
                lastException = e;
                long backoff = randomWaitInMs(i, initialBackoff);
                log.info("Trying source={} attempt {}/{} failed, waiting {}ms", source, i, maxAttempts, backoff);
                clock.sleep(backoff);
            }
        }
        throw lastException;
    }

    // mockable sleep
    public static class Time {
        void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }
}
