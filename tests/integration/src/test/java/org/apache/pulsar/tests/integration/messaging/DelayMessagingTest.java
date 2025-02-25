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
package org.apache.pulsar.tests.integration.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Delay messaging test.
 */
@Slf4j
public class DelayMessagingTest extends PulsarTestSuite {

    DelayMessaging test;
    @BeforeClass(alwaysRun = true)
    public void setupTest() throws Exception {
        this.test = new DelayMessaging(getPulsarClient(), getPulsarAdmin());
    }

    @AfterClass(alwaysRun = true)
    public void closeTest() throws Exception {
        this.test.close();
    }

    @Test
    public void delayMsgBlockTest() throws Exception {
        test.delayMsgBlockTest();
    }

}
