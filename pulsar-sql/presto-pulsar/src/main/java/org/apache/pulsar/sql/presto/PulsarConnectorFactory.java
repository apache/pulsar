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

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import java.util.Map;

/**
 * The factory class which helps to build the presto connector.
 */
public class PulsarConnectorFactory implements ConnectorFactory {

    private static final Logger log = Logger.get(PulsarConnectorFactory.class);

    @Override
    public String getName() {
        return "pulsar";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new PulsarHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "requiredConfig is null");
        if (log.isDebugEnabled()) {
            log.debug("Creating Pulsar connector with configs: %s", config);
        }
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PulsarConnectorModule(connectorId, context.getTypeManager())
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            PulsarConnector connector = injector.getInstance(PulsarConnector.class);
            connector.initConnectorCache();
            return connector;
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
