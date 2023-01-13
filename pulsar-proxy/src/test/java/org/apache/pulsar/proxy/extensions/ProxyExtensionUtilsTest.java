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
package org.apache.pulsar.proxy.extensions;

import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.pulsar.proxy.extensions.ProxyExtensionsUtils.PROXY_EXTENSION_DEFINITION_FILE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@Test(groups = "broker")
public class ProxyExtensionUtilsTest {

    @Test
    public void testLoadProtocolHandler() throws Exception {
        ProxyExtensionDefinition def = new ProxyExtensionDefinition();
        def.setExtensionClass(MockProxyExtension.class.getName());
        def.setDescription("test-ext");

        String archivePath = "/path/to/ext/nar";

        ProxyExtensionMetadata metadata = new ProxyExtensionMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PROXY_EXTENSION_DEFINITION_FILE)))
            .thenReturn(ObjectMapperFactory.getYamlMapper().writer().writeValueAsString(def));
        Class handlerClass = MockProxyExtension.class;
        when(mockLoader.loadClass(eq(MockProxyExtension.class.getName())))
            .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);


            ProxyExtensionWithClassLoader returnedPhWithCL = ProxyExtensionsUtils.load(metadata, "");
            ProxyExtension returnedPh = returnedPhWithCL.getExtension();

            assertSame(mockLoader, returnedPhWithCL.getClassLoader());
            assertTrue(returnedPh instanceof MockProxyExtension);
        }
    }

    @Test
    public void testLoadProtocolHandlerBlankHandlerClass() throws Exception {
        ProxyExtensionDefinition def = new ProxyExtensionDefinition();
        def.setDescription("test-ext");

        String archivePath = "/path/to/ext/nar";

        ProxyExtensionMetadata metadata = new ProxyExtensionMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PROXY_EXTENSION_DEFINITION_FILE)))
            .thenReturn(ObjectMapperFactory.getYamlMapper().writer().writeValueAsString(def));
        Class handlerClass = MockProxyExtension.class;
        when(mockLoader.loadClass(eq(MockProxyExtension.class.getName())))
            .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            try {
                ProxyExtensionsUtils.load(metadata, "");
                fail("Should not reach here");
            } catch (IOException ioe) {
                // expected
            }
        }
    }

    @Test
    public void testLoadProtocolHandlerWrongHandlerClass() throws Exception {
        ProxyExtensionDefinition def = new ProxyExtensionDefinition();
        def.setExtensionClass(Runnable.class.getName());
        def.setDescription("test-ext");

        String archivePath = "/path/to/ext/nar";

        ProxyExtensionMetadata metadata = new ProxyExtensionMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(PROXY_EXTENSION_DEFINITION_FILE)))
            .thenReturn(ObjectMapperFactory.getYamlMapper().writer().writeValueAsString(def));
        Class handlerClass = Runnable.class;
        when(mockLoader.loadClass(eq(Runnable.class.getName())))
            .thenReturn(handlerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            try {
                ProxyExtensionsUtils.load(metadata, "");
                fail("Should not reach here");
            } catch (IOException ioe) {
                // expected
            }
        }
    }

}
