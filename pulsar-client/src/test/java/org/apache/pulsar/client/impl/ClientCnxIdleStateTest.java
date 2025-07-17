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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class ClientCnxIdleStateTest {

    @Test
    public void testShouldNotReleaseConnectionIfIdleCheckFails() throws InterruptedException {
        ClientCnx clientCnx = mock(ClientCnx.class);
        ClientCnxIdleState idleState = new ClientCnxIdleState(clientCnx);
        int maxIdleSeconds = 1;

        // initially, return true for idle check
        doReturn(true).when(clientCnx).idleCheck();

        // do the first idle detection
        idleState.doIdleDetect(maxIdleSeconds);

        // the state should be IDLE since the idle check passed
        assertTrue(idleState.isIdle());

        // Wait for more than maxIdleSeconds
        Thread.sleep(TimeUnit.SECONDS.toMillis(maxIdleSeconds) + 1);

        // now return false for idle check
        doReturn(false).when(clientCnx).idleCheck();

        // do the second idle detection
        idleState.doIdleDetect(maxIdleSeconds);

        // the state should now be USING since the idle check failed
        assertTrue(idleState.isUsing());

        // verify that idleCheck was called twice
        verify(clientCnx, times(2)).idleCheck();
    }
}