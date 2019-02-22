/**
 *
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
 *
 */
package org.apache.pulsar.common.sasl;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * SASL Constants.
 */
public class SaslConstants {

    public static final String AUTH_METHOD_NAME = "sasl";

    // service broker Principal
    public static final String JAAS_BROKER_SECTION_NAME = "saslJaasBrokerSectionName";
    public static final String JAAS_DEFAULT_BROKER_SECTION_NAME = "Broker";

    // TODO: Proxy principal
    public static final String JAAS_PROXY_SECTION_NAME = "saslJaasProxySectionName";
    public static final String JAAS_DEFAULT_PROXY_SECTION_NAME = "Proxy";

    // Client principal
    public static final String JAAS_CLIENT_SECTION_NAME = "saslJaasClientSectionName";
    public static final String JAAS_DEFAULT_CLIENT_SECTION_NAME = "Client";

    /**
     * This is a regexp which limits the range of possible ids which can connect to the Broker using SASL.
     * By default only clients whose id contains 'pulsar' are allowed to connect.
     */
    public static final String JAAS_CLIENT_ALLOWED_IDS = "saslJaasClientAllowedIds";
    public static final String JAAS_CLIENT_ALLOWED_IDS_DEFAULT = ".*pulsar.*";

    public static final String KINIT_COMMAND_DEFAULT = "/usr/bin/kinit";

    public static final String KINIT_COMMAND = "kerberos.kinit";

    // The non-null string name of the protocol for which the authentication is being performed (e.g., "ldap").
    public static final String SASL_PULSAR_PROTOCOL = "broker";
    // The non-null fully-qualified host name of the server to authenticate to.
    public static final String SASL_PULSAR_REALM = "EXAMPLE.COM";

    // Stand for the start of mutual auth between Client and Broker
    public static final String INIT_PROVIDER_DATA = "isInit";

    public static boolean isUsingTicketCache(String configurationEntry) {
        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(configurationEntry);
        if (entries == null) {
            return false;
        }
        for (AppConfigurationEntry entry : entries) {
            // there will only be a single entry, so this for() loop will only be iterated through once.
            if (entry.getOptions().get("useTicketCache") != null) {
                String val = (String) entry.getOptions().get("useTicketCache");
                if (val.equals("true")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getPrincipal(String configurationEntry) {

        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(configurationEntry);
        if (entries == null) {
            return null;
        }
        for (AppConfigurationEntry entry : entries) {
            if (entry.getOptions().get("principal") != null) {
                return (String) entry.getOptions().get("principal");
            }
        }
        return null;
    }
}
