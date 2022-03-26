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
package org.apache.pulsar.broker.service;

import java.util.Set;
import java.util.concurrent.Semaphore;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ServeCnxForSemaphoreReleaseTest {

    @Test
    public void testHandleLookupForSemaphoreRelease(){
        PulsarService pulsarService = Mockito.mock(PulsarService.class);
        // mock broker service
        BrokerService service = Mockito.mock(BrokerService.class);
        Mockito.when(pulsarService.getBrokerService()).thenReturn(service);
        // mock ServiceConfiguration
        ServiceConfiguration conf = Mockito.mock(ServiceConfiguration.class);
        Set<String> proxyRoles = Mockito.mock(Set.class);
        Mockito.when(pulsarService.getConfiguration()).thenReturn(conf);
        Mockito.when(conf.getProxyRoles()).thenReturn(proxyRoles);
        Mockito.when(conf.getMaxMessagePublishBufferSizeInMB()).thenReturn(10);
        Mockito.when(conf.getNumIOThreads()).thenReturn(1);
        // new  serverCnx
        ServerCnx serverCnx = new ServerCnx(pulsarService, "listenerName");
        Semaphore lookupSemaphore = new Semaphore(10);
        CommandLookupTopic lookup = Mockito.mock(CommandLookupTopic.class);
        Mockito.when(lookup.getTopic()).thenReturn("topic1");
        Mockito.when(service.getLookupRequestSemaphore()).thenReturn(lookupSemaphore);
        Mockito.when(service.isAuthenticationEnabled()).thenReturn(true);
        Mockito.when(service.isAuthorizationEnabled()).thenReturn(true);
        Mockito.when(proxyRoles.contains(null)).thenReturn(true);
        for(int i = 0;i < 10; i++){
            try {
                serverCnx.handleLookup(lookup);
            }catch (NullPointerException ne){

            }
        }
        Assert.assertEquals(10, lookupSemaphore.availablePermits());
    }
}
