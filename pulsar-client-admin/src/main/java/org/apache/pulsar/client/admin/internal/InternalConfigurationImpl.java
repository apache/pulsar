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
package org.apache.pulsar.client.admin.internal;

import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.InternalConfiguration;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.conf.InternalConfigurationData;

public class InternalConfigurationImpl extends BaseResource implements InternalConfiguration {

    private final WebTarget internalConfiguration;

    public InternalConfigurationImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.internalConfiguration = web.path("/internal-configuration");
    }

    @Override
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        try {
            return request(internalConfiguration).get(InternalConfigurationData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
