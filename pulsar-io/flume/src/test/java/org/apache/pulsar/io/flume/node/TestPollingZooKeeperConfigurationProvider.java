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
package org.apache.pulsar.io.flume.node;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPollingZooKeeperConfigurationProvider extends
        TestAbstractZooKeeperConfigurationProvider {

    private EventBus eb;

    private EventSync es;

    private PollingZooKeeperConfigurationProvider cp;

    private class EventSync {

        private boolean notified;

        @Subscribe
        public synchronized void notifyEvent(MaterializedConfiguration mConfig) {
            notified = true;
            notifyAll();
        }

        public synchronized void awaitEvent() throws InterruptedException {
            while (!notified) {
                wait();
            }
        }

        public synchronized void reset() {
            notified = false;
        }
    }

    @Override
    protected void doSetUp() throws Exception {
        eb = new EventBus("test");
        es = new EventSync();
        es.reset();
        eb.register(es);
        cp = new PollingZooKeeperConfigurationProvider(AGENT_NAME, "localhost:"
                + zkServer.getPort(), null, eb);
        cp.start();
        LifecycleController.waitForOneOf(cp, LifecycleState.START_OR_ERROR);
    }

    @Override
    protected void doTearDown() {
        // do nothing
    }

    @Test
    public void testPolling() throws Exception {
        es.awaitEvent();
        es.reset();

        FlumeConfiguration fc = cp.getFlumeConfiguration();
        Assert.assertTrue(fc.getConfigurationErrors().isEmpty());
        AgentConfiguration ac = fc.getConfigurationFor(AGENT_NAME);
        Assert.assertNull(ac);

        addData();
        es.awaitEvent();
        es.reset();

        verifyProperties(cp);
    }
}
