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

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.decoder.DecoderModule;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import javax.inject.Inject;

/**
 * This class defines binding of classes in the Presto connector.
 */
public class PulsarConnectorModule implements Module {

    private final String connectorId;
    private final TypeManager typeManager;

    public PulsarConnectorModule(String connectorId, TypeManager typeManager) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(PulsarConnector.class).in(Scopes.SINGLETON);
        binder.bind(PulsarConnectorId.class).toInstance(new PulsarConnectorId(connectorId));

        binder.bind(PulsarMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PulsarSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PulsarRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(PulsarDispatchingRowDecoderFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(PulsarConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);

        binder.install(new DecoderModule());

    }

    /**
     * A wrapper to deserialize the Presto types.
     */
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type> {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager) {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            return typeManager.getType(TypeId.of(value));
        }
    }
}
