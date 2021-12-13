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
package org.apache.pulsar.admin.cli;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Worker;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestCmdFunctionWorker {

    private PulsarAdmin pulsarAdmin;

    private CmdFunctionWorker cmdFunctionWorker;

    private Worker worker;

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        worker = mock(Worker.class);
        when(pulsarAdmin.worker()).thenReturn(worker);

        cmdFunctionWorker = spy(new CmdFunctionWorker(() -> pulsarAdmin));
    }

    @Test
    public void testCmdRebalance() throws Exception {
        cmdFunctionWorker.run(new String[]{"rebalance"});
        verify(pulsarAdmin.worker(), times(1)).rebalance();
    }
}
