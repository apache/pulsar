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
package org.apache.pulsar.proxy.server.plugin;

import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.proxy.server.plugin.servlet.ProxyAdditionalServlet;
import org.apache.pulsar.proxy.server.plugin.servlet.ProxyAdditionalServletWithClassLoader;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

/**
 * Unit test {@link ProxyAdditionalServletWithClassLoader}.
 */
public class ProxyAdditionalServletWithClassLoaderTest {

    @Test
    public void testWrapper() throws Exception {
        ProxyAdditionalServlet servlet = mock(ProxyAdditionalServlet.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        ProxyAdditionalServletWithClassLoader wrapper = new ProxyAdditionalServletWithClassLoader(servlet, loader);

        String basePath = "metrics/pulsar";

        when(servlet.getBasePath()).thenReturn(basePath);
        assertEquals(basePath, wrapper.getBasePath());
        verify(servlet, times(1)).getBasePath();
    }
}
