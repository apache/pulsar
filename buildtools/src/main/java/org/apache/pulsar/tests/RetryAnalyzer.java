/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests;

import org.apache.commons.lang3.math.NumberUtils;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

public class RetryAnalyzer implements IRetryAnalyzer {

    private int retryCount = 1;

    // Only try again once
    private static final int MAX_RETRIES_SYSTEM = Integer.parseInt(System.getProperty("testRetryCount", "1"));
    private static final int MAX_RETRIES_ENV = NumberUtils.toInt(System.getenv("TEST_RETRY_COUNT"), 1);
    private static final int MAX_RETRIES_COUNT = Math.max(MAX_RETRIES_SYSTEM, MAX_RETRIES_ENV);

    @Override
    public boolean retry(ITestResult result) {

        if (retryCount <= MAX_RETRIES_COUNT) {
            retryCount++;
            String message = "running retry for '" + result.getName() + "' on class " + this.getClass().getName()
                    + " Retrying " + retryCount + " times";
            System.out.println(message);
            return true;
        }
        return false;
    }

}
