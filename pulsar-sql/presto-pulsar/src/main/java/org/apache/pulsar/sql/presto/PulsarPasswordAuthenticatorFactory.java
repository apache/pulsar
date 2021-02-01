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
package org.apache.pulsar.sql.presto;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.prestosql.spi.security.PasswordAuthenticator;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class PulsarPasswordAuthenticatorFactory implements PasswordAuthenticatorFactory {

    private static final Logger log = Logger.get(PulsarPasswordAuthenticatorFactory.class);

    @Override
    public String getName() {
        return "pulsar";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config) {
        log.info("Creating password authenticator.");
        if (log.isDebugEnabled()){
            log.debug("Creating Pulsar password authenticator with configs: %s", config);
        }
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new PulsarPasswordAuthenticatorModule()
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            PulsarPasswordAuthenticator authenticator = injector.getInstance(PulsarPasswordAuthenticator.class);
            return authenticator;
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
