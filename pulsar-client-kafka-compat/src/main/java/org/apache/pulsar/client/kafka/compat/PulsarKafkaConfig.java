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
package org.apache.pulsar.client.kafka.compat;

import java.util.Properties;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientConfiguration;

public class PulsarKafkaConfig {

    /// Config variables
    public static final String AUTHENTICATION_CLASS = "pulsar.authentication.class";
    public static final String USE_TLS = "pulsar.use.tls";
    public static final String TLS_TRUST_CERTS_FILE_PATH = "pulsar.tls.trust.certs.file.path";
    public static final String TLS_ALLOW_INSECURE_CONNECTION = "pulsar.tls.allow.insecure.connection";

    public static ClientConfiguration getClientConfiguration(Properties properties) {
        ClientConfiguration conf = new ClientConfiguration();

        if (properties.containsKey(AUTHENTICATION_CLASS)) {
            String className = properties.getProperty(AUTHENTICATION_CLASS);
            try {
                @SuppressWarnings("unchecked")
                Class<Authentication> clazz = (Class<Authentication>) Class.forName(className);
                Authentication auth = clazz.newInstance();
                conf.setAuthentication(auth);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        conf.setUseTls(Boolean.parseBoolean(properties.getProperty(USE_TLS, "false")));
        conf.setUseTls(Boolean.parseBoolean(properties.getProperty(TLS_ALLOW_INSECURE_CONNECTION, "false")));
        if (properties.containsKey(TLS_TRUST_CERTS_FILE_PATH)) {
            conf.setTlsTrustCertsFilePath(properties.getProperty(TLS_TRUST_CERTS_FILE_PATH));
        }

        return conf;
    }
}
