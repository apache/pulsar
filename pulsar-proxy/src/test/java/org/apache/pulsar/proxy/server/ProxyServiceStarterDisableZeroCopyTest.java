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
package org.apache.pulsar.proxy.server;

import java.util.Optional;
import org.testng.annotations.BeforeClass;

public class ProxyServiceStarterDisableZeroCopyTest extends ProxyServiceStarterTest{

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
        serviceStarter = new ProxyServiceStarter(ARGS);
        serviceStarter.getConfig().setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        serviceStarter.getConfig().setBrokerWebServiceURL(pulsar.getWebServiceAddress());
        serviceStarter.getConfig().setWebServicePort(Optional.of(0));
        serviceStarter.getConfig().setServicePort(Optional.of(0));
        serviceStarter.getConfig().setWebSocketServiceEnabled(true);
        serviceStarter.getConfig().setBrokerProxyAllowedTargetPorts("*");
        serviceStarter.getConfig().setProxyZeroCopyModeEnabled(false);
        serviceStarter.start();
        serviceUrl = serviceStarter.getProxyService().getServiceUrl();
    }
}
