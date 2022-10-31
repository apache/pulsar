/*
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
package org.apache.pulsar.tests.integration.backwardscompatibility;

import java.util.function.Supplier;
import org.testng.annotations.Test;

public class SmokeTest2_4 extends PulsarStandaloneTestSuite2_4 {

    @Test(dataProvider = "StandaloneServiceUrlAndTopics")
    public void testPublishAndConsume(Supplier<String> serviceUrl, boolean isPersistent) throws Exception {
        super.testPublishAndConsume(serviceUrl.get(), isPersistent);
    }

    @Test(dataProvider = "StandaloneServiceUrlAndTopics")
    public void testBatchMessagePublishAndConsume(Supplier<String> serviceUrl, boolean isPersistent) throws Exception {
        super.testBatchMessagePublishAndConsume(serviceUrl.get(), isPersistent);
    }

    @Test(dataProvider = "StandaloneServiceUrlAndTopics")
    public void testBatchIndexAckDisabled(Supplier<String> serviceUrl, boolean isPersistent) throws Exception {
        super.testBatchIndexAckDisabled(serviceUrl.get());
    }
}
