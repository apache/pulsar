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
package org.apache.pulsar.broker.protocol;

import static org.apache.pulsar.broker.protocol.ProtocolHandlerUtils.PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ProtocolHandlerUtilsTest {

    @Test
    public void testLoadProtocolHandler() throws Exception {
        ProtocolHandlerDefinition def = new ProtocolHandlerDefinition();
        def.setHandlerClass(MockProtocolHandler.class.getName());
        def.setDescription("test-protocol-handler");

        String archivePath = "/path/to/protocol/handler/nar";

        ProtocolHandlerMetadata metadata = new ProtocolHandlerMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE)))
            .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class handlerClass = MockProtocolHandler.class;
        when(mockLoader.loadClass(eq(MockProtocolHandler.class.getName())))
            .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            ProtocolHandlerWithClassLoader returnedPhWithCL = ProtocolHandlerUtils.load(metadata, "");
            ProtocolHandler returnedPh = returnedPhWithCL.getHandler();

            assertSame(mockLoader, returnedPhWithCL.getClassLoader());
            assertTrue(returnedPh instanceof MockProtocolHandler);
        }
    }

    @Test
    public void testLoadProtocolHandlerBlankHandlerClass() throws Exception {
        ProtocolHandlerDefinition def = new ProtocolHandlerDefinition();
        def.setDescription("test-protocol-handler");

        String archivePath = "/path/to/protocol/handler/nar";

        ProtocolHandlerMetadata metadata = new ProtocolHandlerMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE)))
                .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class handlerClass = MockProtocolHandler.class;
        when(mockLoader.loadClass(eq(MockProtocolHandler.class.getName())))
                .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            try {
                ProtocolHandlerUtils.load(metadata, "");
                fail("Should not reach here");
            } catch (IOException ioe) {
                // expected
            }
        }
    }

    @Test
    public void testLoadProtocolHandlerWrongHandlerClass() throws Exception {
        ProtocolHandlerDefinition def = new ProtocolHandlerDefinition();
        def.setHandlerClass(Runnable.class.getName());
        def.setDescription("test-protocol-handler");

        String archivePath = "/path/to/protocol/handler/nar";

        ProtocolHandlerMetadata metadata = new ProtocolHandlerMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE)))
                .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class handlerClass = Runnable.class;
        when(mockLoader.loadClass(eq(Runnable.class.getName())))
                .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            try {
                ProtocolHandlerUtils.load(metadata, "");
                fail("Should not reach here");
            } catch (IOException ioe) {
                // expected
            }
        }
    }

}
