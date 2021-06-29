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
package org.apache.pulsar.tests;

import org.testng.annotations.Test;

public class RetryAnalyzerTest {
    private static final int RETRY_COUNT = 3;

    public static class TestRetryAnalyzer extends RetryAnalyzer {
        public TestRetryAnalyzer() {
            setCount(RETRY_COUNT);
        }
    }
    int invocationCountA;
    int invocationCountB;
    int invocationCountC;

    @Test(retryAnalyzer = TestRetryAnalyzer.class)
    void testMethodA() {
        invocationCountA++;
        if (invocationCountA < RETRY_COUNT) {
            throw new IllegalStateException("Sample failure to trigger retry.");
        }
    }

    @Test(retryAnalyzer = TestRetryAnalyzer.class)
    void testMethodB() {
        invocationCountB++;
        if (invocationCountB < RETRY_COUNT) {
            throw new IllegalStateException("Sample failure to trigger retry.");
        }
    }

    @Test(retryAnalyzer = TestRetryAnalyzer.class)
    void testMethodC() {
        invocationCountC++;
        if (invocationCountC < RETRY_COUNT) {
            throw new IllegalStateException("Sample failure to trigger retry.");
        }
    }
}