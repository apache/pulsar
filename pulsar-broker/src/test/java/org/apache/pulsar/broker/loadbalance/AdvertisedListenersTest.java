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
package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URI;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.PortManager;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class AdvertisedListenersTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        updateConfig(conf, "BROKER-X");
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        ServiceConfiguration conf = super.createConfForAdditionalBroker(additionalBrokerIndex);
        updateConfig(conf, "BROKER-" + additionalBrokerIndex);
        return conf;
    }

    private void updateConfig(ServiceConfiguration conf, String advertisedAddress) {
        int pulsarPort = PortManager.nextFreePort();
        int httpPort = PortManager.nextFreePort();
        int httpsPort = PortManager.nextFreePort();

        // Use invalid domain name as identifier and instead make sure the advertised listeners work as intended
        conf.setAdvertisedAddress(advertisedAddress);
        conf.setAdvertisedListeners(
                "public:pulsar://localhost:" + pulsarPort +
                        ",public_http:http://localhost:" + httpPort +
                        ",public_https:https://localhost:" + httpsPort);
        conf.setBrokerServicePort(Optional.of(pulsarPort));
        conf.setWebServicePort(Optional.of(httpPort));
        conf.setWebServicePortTls(Optional.of(httpsPort));
    }

    @Test
    public void testLookup() throws Exception {
        HttpGet request =
                new HttpGet(pulsar.getWebServiceAddress() + "/lookup/v2/topic/persistent/public/default/my-topic");
        request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        final String topic = "my-topic";

        @Cleanup
        CloseableHttpClient httpClient = HttpClients.createDefault();

        @Cleanup
        CloseableHttpResponse response = httpClient.execute(request);

        HttpEntity entity = response.getEntity();
        LookupData ld = ObjectMapperFactory.getThreadLocal().readValue(EntityUtils.toString(entity), LookupData.class);
        System.err.println("Lookup data: " + ld);

        assertEquals(new URI(ld.getBrokerUrl()).getHost(), "localhost");
        assertEquals(new URI(ld.getHttpUrl()).getHost(), "localhost");
        assertEquals(new URI(ld.getHttpUrlTls()).getHost(), "localhost");


        // Produce data
        @Cleanup
        Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        p.send("hello");

        // Verify we can get the correct HTTP redirect to the advertised listener
        for (PulsarAdmin a : getAllAdmins()) {
            TopicStats s = a.topics().getStats(topic);
            assertNotNull(a.lookups().lookupTopic(topic));
            assertEquals(s.getPublishers().size(), 1);
        }
    }

}
