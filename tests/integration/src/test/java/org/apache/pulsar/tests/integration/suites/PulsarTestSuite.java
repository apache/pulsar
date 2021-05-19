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
package org.apache.pulsar.tests.integration.suites;

import java.util.function.Predicate;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class PulsarTestSuite extends PulsarClusterTestBase {

    @BeforeClass(alwaysRun = true)
    public final void setupBeforeClass() throws Exception {
        setup();
    }

    @AfterClass(alwaysRun = true)
    public final void tearDownAfterClass() throws Exception {
        cleanup();
    }

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis) throws Exception {
        retryStrategically(predicate, retryCount, intSleepTimeInMillis, false);
    }

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis, boolean throwException)
            throws Exception {

        for (int i = 0; i < retryCount; i++) {
            if (throwException) {
                if (i == (retryCount - 1)) {
                    throw new RuntimeException("Action was not successful after " + retryCount + " retries");
                }
                if (predicate.test(null)) {
                    break;
                }
            } else {
                if (predicate.test(null) || i == (retryCount - 1)) {
                    break;
                }
            }

           Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }
}
