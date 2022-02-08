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
package org.apache.pulsar.broker.web.plugin.servlet;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

public class AdditionalServletUtilsTest {

    @Test
    public void testLoadEventListener() throws Exception {
        AdditionalServletDefinition def = new AdditionalServletDefinition();
        def.setAdditionalServletClass(MockAdditionalServlet.class.getName());
        def.setDescription("test-proxy-listener");

        String archivePath = "/path/to/proxy/listener/nar";

        AdditionalServletMetadata metadata = new AdditionalServletMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(AdditionalServletUtils.ADDITIONAL_SERVLET_FILE)))
                .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class listenerClass = MockAdditionalServlet.class;
        when(mockLoader.loadClass(eq(MockAdditionalServlet.class.getName())))
                .thenReturn(listenerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);
            AdditionalServletWithClassLoader returnedPhWithCL = AdditionalServletUtils.load(metadata, "");
            AdditionalServlet returnedPh = returnedPhWithCL.getServlet();

            assertSame(mockLoader, returnedPhWithCL.getClassLoader());
            assertTrue(returnedPh instanceof MockAdditionalServlet);
        }
    }

    @Test(expectedExceptions = IOException.class)
    public void testLoadEventListenerWithBlankListerClass() throws Exception {
        AdditionalServletDefinition def = new AdditionalServletDefinition();
        def.setDescription("test-proxy-listener");

        String archivePath = "/path/to/proxy/listener/nar";

        AdditionalServletMetadata metadata = new AdditionalServletMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(AdditionalServletUtils.ADDITIONAL_SERVLET_FILE)))
                .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class listenerClass = MockAdditionalServlet.class;
        when(mockLoader.loadClass(eq(MockAdditionalServlet.class.getName())))
                .thenReturn(listenerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            AdditionalServletUtils.load(metadata, "");
        }
    }

    @Test(expectedExceptions = IOException.class)
    public void testLoadEventListenerWithWrongListerClass() throws Exception {
        AdditionalServletDefinition def = new AdditionalServletDefinition();
        def.setAdditionalServletClass(Runnable.class.getName());
        def.setDescription("test-proxy-listener");

        String archivePath = "/path/to/proxy/listener/nar";

        AdditionalServletMetadata metadata = new AdditionalServletMetadata();
        metadata.setDefinition(def);
        metadata.setArchivePath(Paths.get(archivePath));

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        when(mockLoader.getServiceDefinition(eq(AdditionalServletUtils.ADDITIONAL_SERVLET_FILE)))
                .thenReturn(ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(def));
        Class listenerClass = Runnable.class;
        when(mockLoader.loadClass(eq(Runnable.class.getName())))
                .thenReturn(listenerClass);

        final NarClassLoaderBuilder mockedBuilder = mock(NarClassLoaderBuilder.class, RETURNS_SELF);
        when(mockedBuilder.build()).thenReturn(mockLoader);
        try (MockedStatic<NarClassLoaderBuilder> builder = Mockito.mockStatic(NarClassLoaderBuilder.class)) {
            builder.when(() -> NarClassLoaderBuilder.builder()).thenReturn(mockedBuilder);

            AdditionalServletUtils.load(metadata, "");
        }
    }
}
