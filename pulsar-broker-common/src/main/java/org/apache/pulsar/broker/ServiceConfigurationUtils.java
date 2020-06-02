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
package org.apache.pulsar.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class ServiceConfigurationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceConfigurationUtils.class);

    public static String getDefaultOrConfiguredAddress(String configuredAddress) {
        if (isBlank(configuredAddress)) {
            return unsafeLocalhostResolve();
        }
        return configuredAddress;
    }

    public static String unsafeLocalhostResolve() {
        try {
            // Get the fully qualified hostname
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException("Failed to resolve localhost name.", ex);
        }
    }

}
