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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

@PrepareForTest({
        NarClassLoader.class
})
@PowerMockIgnore({"org.apache.logging.log4j.*"})
public class AdditionalServletUtilsTest {

    // Necessary to make PowerMockito.mockStatic work with TestNG.
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

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

        PowerMockito.mockStatic(NarClassLoader.class);
        PowerMockito.when(NarClassLoader.getFromArchive(
                any(File.class),
                any(Set.class),
                any(ClassLoader.class),
                any(String.class)
        )).thenReturn(mockLoader);

        AdditionalServletWithClassLoader returnedPhWithCL = AdditionalServletUtils.load(metadata, "");
        AdditionalServlet returnedPh = returnedPhWithCL.getServlet();

        assertSame(mockLoader, returnedPhWithCL.getClassLoader());
        assertTrue(returnedPh instanceof MockAdditionalServlet);
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

        PowerMockito.mockStatic(NarClassLoader.class);
        PowerMockito.when(NarClassLoader.getFromArchive(
                any(File.class),
                any(Set.class),
                any(ClassLoader.class),
                any(String.class)
        )).thenReturn(mockLoader);

        AdditionalServletUtils.load(metadata, "");
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

        PowerMockito.mockStatic(NarClassLoader.class);
        PowerMockito.when(NarClassLoader.getFromArchive(
                any(File.class),
                any(Set.class),
                any(ClassLoader.class),
                any(String.class)
        )).thenReturn(mockLoader);

        AdditionalServletUtils.load(metadata, "");
    }
}
