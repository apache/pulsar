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
package org.apache.pulsar.broker.loadbalance;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class AdvertisedListenersMultiBrokerLeaderElectionTest extends MultiBrokerLeaderElectionTest {
    @Override
    protected PulsarTestContext.Builder createPulsarTestContextBuilder(ServiceConfiguration conf) {
        conf.setWebServicePortTls(Optional.of(0));
        return super.createPulsarTestContextBuilder(conf).preallocatePorts(true).configOverride(config -> {
            // use advertised address that is different than the name used in the advertised listeners
            config.setAdvertisedAddress("localhost");
            config.setAdvertisedListeners(
                    "public_pulsar:pulsar://127.0.0.1:" + config.getBrokerServicePort().get()
                            + ",public_http:http://127.0.0.1:" + config.getWebServicePort().get()
                            + ",public_https:https://127.0.0.1:" + config.getWebServicePortTls().get());
        });
    }
}
