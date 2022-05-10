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

package org.apache.pulsar.common.sasl;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * SASL Constants.
 */
public class SaslConstants {

    public static final String AUTH_METHOD_NAME = "sasl";

    // service section name, this is broker or proxy Principal
    public static final String JAAS_SERVER_SECTION_NAME = "saslJaasServerSectionName";
    public static final String JAAS_DEFAULT_BROKER_SECTION_NAME = "PulsarBroker";
    public static final String JAAS_DEFAULT_PROXY_SECTION_NAME = "PulsarProxy";
    public static final String JAAS_DEFAULT_FUNCTION_SECTION_NAME = "PulsarFunction";

    // Client principal
    public static final String JAAS_CLIENT_SECTION_NAME = "saslJaasClientSectionName";
    public static final String JAAS_DEFAULT_CLIENT_SECTION_NAME = "PulsarClient";

    /**
     * This is a regexp which limits the range of possible ids which can connect to the Broker using SASL.
     * By default only clients whose id contains 'pulsar' are allowed to connect.
     */
    public static final String JAAS_CLIENT_ALLOWED_IDS = "saslJaasClientAllowedIds";
    public static final String JAAS_CLIENT_ALLOWED_IDS_DEFAULT = ".*pulsar.*";

    public static final String KINIT_COMMAND_DEFAULT = "/usr/bin/kinit";

    public static final String KINIT_COMMAND = "kerberos.kinit";

    // The sasl server type that client will connect to. default value broker, could also be proxy.
    public static final String SASL_SERVER_TYPE = "serverType";
    // The non-null string name of the protocol for which the authentication is being performed (e.g., "ldap").
    public static final String SASL_BROKER_PROTOCOL = "broker";
    public static final String SASL_PROXY_PROTOCOL = "proxy";
    // The non-null fully-qualified host name of the server to authenticate to.
    public static final String SASL_PULSAR_REALM = "EXAMPLE.COM";

    // Stand for the start of mutual auth between Client and Broker
    public static final String INIT_PROVIDER_DATA = "isInit";


    // Sasl token name that contained auth role
    public static final String SASL_AUTH_ROLE_TOKEN = "SaslAuthRoleToken";
    public static final String SASL_AUTH_ROLE_TOKEN_EXPIRED = "SaslAuthRoleTokenExpired";

    /**
     * HTTP header used by the SASL client/server during an authentication sequence.
     */
    // auth type
    public static final String SASL_HEADER_TYPE = "SASL-Type";
    public static final String SASL_TYPE_VALUE = "Kerberos";

    // header name for token auth between client and server
    public static final String SASL_AUTH_TOKEN = "SASL-Token";

    // header name for state
    public static final String SASL_HEADER_STATE = "State";
    // header value for state
    public static final String SASL_STATE_CLIENT_INIT = "Init";
    public static final String SASL_STATE_NEGOTIATE = "ING";
    public static final String SASL_STATE_COMPLETE = "Done";
    public static final String SASL_STATE_SERVER_CHECK_TOKEN = "ServerCheckToken";

    // server side track the server
    public static final String SASL_STATE_SERVER = "SASL-Server-ID";

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
