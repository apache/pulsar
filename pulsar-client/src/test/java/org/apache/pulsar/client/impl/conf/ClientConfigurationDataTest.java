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

package org.apache.pulsar.client.impl.conf;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ClientConfigurationData}.
 */
public class ClientConfigurationDataTest {

    @Test
    public void testDoNotPrintSensitiveInfo() throws JsonProcessingException {
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setTlsTrustStorePassword("xxxx");
        clientConfigurationData.setSocks5ProxyPassword("yyyy");
        clientConfigurationData.setAuthentication(new AuthenticationToken("zzzz"));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String serializedConf = objectMapper.writeValueAsString(clientConfigurationData);
        assertThat(serializedConf).doesNotContain("xxxx", "yyyy", "zzzz");
    }

}
