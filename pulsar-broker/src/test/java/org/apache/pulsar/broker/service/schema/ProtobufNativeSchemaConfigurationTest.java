/*
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
package org.apache.pulsar.broker.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import java.util.Set;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ProtobufNativeSchemaConfigurationTest {
    private static final String ADVANCED = ProtobufNativeSchemaAdvancedCompatibilityCheck.class.getName();
    private static final String LEGACY = ProtobufNativeSchemaCompatibilityCheck.class.getName();

    @Test
    public void testSelectionAndConflict() throws Exception {
        assertThat(SchemaRegistryService.getCheckers(Set.of(LEGACY)).get(SchemaType.PROTOBUF_NATIVE))
                .isInstanceOf(ProtobufNativeSchemaCompatibilityCheck.class);
        assertThat(SchemaRegistryService.getCheckers(Set.of(ADVANCED)).get(SchemaType.PROTOBUF_NATIVE))
                .isInstanceOf(ProtobufNativeSchemaAdvancedCompatibilityCheck.class);
        assertThatThrownBy(() -> SchemaRegistryService.create(mock(SchemaStorage.class),
                Set.of(LEGACY, ADVANCED), mock(PulsarService.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only one PROTOBUF_NATIVE");
        assertThatThrownBy(() -> SchemaRegistryService.create(null, Set.of(ADVANCED),
                mock(PulsarService.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires schema storage");
    }

    @Test
    public void testCustomNativeConflict() {
        assertThatThrownBy(() -> SchemaRegistryService.create(mock(SchemaStorage.class),
                Set.of(ADVANCED, AnotherNativeChecker.class.getName()), mock(PulsarService.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Multiple PROTOBUF_NATIVE compatibility checkers configured");
    }

    @Test
    public void testAdvancedClassLoadingFailureDoesNotFallback() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader rejecting = new ClassLoader(original) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals(ADVANCED)) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };
        try {
            Thread.currentThread().setContextClassLoader(rejecting);
            assertThatThrownBy(() -> SchemaRegistryService.create(mock(SchemaStorage.class), Set.of(ADVANCED),
                    mock(PulsarService.class)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Unable to initialize advanced PROTOBUF_NATIVE checker");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    public static class AnotherNativeChecker implements SchemaCompatibilityCheck {
        @Override
        public SchemaType getSchemaType() {
            return SchemaType.PROTOBUF_NATIVE;
        }

        @Override
        public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
                throws IncompatibleSchemaException {
        }

        @Override
        public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
                throws IncompatibleSchemaException {
        }
    }
}
