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
package org.apache.pulsar.websocket;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.io.InputStream;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.annotations.Test;

public class WebSocketServiceTest {

    @Test
    public void testMetadataStoreAllowReadOnlyOperations() throws Exception {
        @Cleanup
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("websocket.conf");
        Properties properties = new Properties();
        properties.load(inputStream);
        properties.setProperty("authenticationEnabled", "false");
        properties.setProperty("authorizationEnabled", "false");
        properties.setProperty("metadataStoreAllowReadOnlyOperations", "true");
        WebSocketProxyConfiguration config = PulsarConfigurationLoader.create(properties,
                WebSocketProxyConfiguration.class);
        @Cleanup
        WebSocketService service = spy(new WebSocketService(config));
        service.start();
        verify(service).createConfigMetadataStore(eq("memory:127.0.0.1:2181"), eq(30000), eq(true));
    }

}
