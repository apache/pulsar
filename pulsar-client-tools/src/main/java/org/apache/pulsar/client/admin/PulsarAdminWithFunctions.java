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
package org.apache.pulsar.client.admin;

import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;


/**
 * Pulsar client admin client with functions support.
 */
public class PulsarAdminWithFunctions extends PulsarAdmin {

    private final Functions functions;
    private final ClientConfigurationData clientConf;

    /**
     * Creates a builder to construct an instance of {@link PulsarAdminWithFunctions}.
     */
    public static PulsarAdminBuilder builder() {
        return new PulsarAdminBuilderImpl();
    }

    PulsarAdminWithFunctions(String serviceUrl, ClientConfigurationData pulsarConfig) throws PulsarClientException {
        super(serviceUrl, pulsarConfig);
        this.functions = new FunctionsImpl(root, auth);
        this.clientConf = pulsarConfig;
    }

    public ClientConfigurationData getClientConf() {
        return clientConf;
    }

    /**
     * @return the function management object
     */
    public Functions functions() {
        return functions;
    }
}
