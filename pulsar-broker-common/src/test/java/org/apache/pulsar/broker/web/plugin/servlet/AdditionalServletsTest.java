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

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

@PrepareForTest({
        AdditionalServletUtils.class
})
@PowerMockIgnore({"org.apache.logging.log4j.*", "javax.xml.*", "com.sun.org.apache.xerces.*"})
public class AdditionalServletsTest extends PowerMockTestCase {


    @Test
    public void testEmptyStringAsExtractionDirectory() throws IOException {
        Properties p = new Properties();
        p.put("narExtractionDirectory", "");
        p.put("additionalServlets", "AS1,AS2");
        p.put("additionalServletDirectory", "/additionalServletDirectory");

        PulsarConfiguration config = mock(PulsarConfiguration.class);
        Mockito.when(config.getProperties()).thenReturn(p);

        AdditionalServletMetadata asm1 = additionalServletMetadata(1);
        AdditionalServletMetadata asm2 = additionalServletMetadata(2);

        AdditionalServletDefinitions definitions = new AdditionalServletDefinitions();
        definitions.servlets().put("AS1", asm1);
        definitions.servlets().put("AS2", asm2);

        AdditionalServletWithClassLoader as1 = mock(AdditionalServletWithClassLoader.class);
        AdditionalServletWithClassLoader as2 = mock(AdditionalServletWithClassLoader.class);

        String originalTmpDirectory = System.getProperty("java.io.tmpdir");

        try {
            PowerMockito.mockStatic(AdditionalServletUtils.class);
            String tmpDirectory = "/my/tmp/directory";
            System.setProperty("java.io.tmpdir", tmpDirectory);
            when(AdditionalServletUtils.searchForServlets(
                    "/additionalServletDirectory", tmpDirectory)).thenReturn(definitions);
            when(AdditionalServletUtils.load(asm1, tmpDirectory)).thenReturn(as1);
            when(AdditionalServletUtils.load(asm2, tmpDirectory)).thenReturn(as2);

            AdditionalServlets servlets = AdditionalServlets.load(config);

            Assert.assertEquals(servlets.getServlets().get("AS1"), as1);
            Assert.assertEquals(servlets.getServlets().get("AS2"), as2);
        } finally {
            System.setProperty("java.io.tmpdir", originalTmpDirectory);
        }
    }

    private AdditionalServletMetadata additionalServletMetadata(int index) {
        AdditionalServletMetadata as = new AdditionalServletMetadata();
        as.setArchivePath(Paths.get("/additionalServletDirectory/" + index));
        as.setDefinition(new AdditionalServletDefinition());
        as.getDefinition().setName("as" + index);
        as.getDefinition().setAdditionalServletClass("com.example.AS" + index);
        as.getDefinition().setDescription("Additional Servlet " +index);
        return as;
    }
}
