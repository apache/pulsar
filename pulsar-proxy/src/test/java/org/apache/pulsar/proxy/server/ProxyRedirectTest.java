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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.testng.annotations.Test;

@Slf4j
public class ProxyRedirectTest extends ProxyMultiBrokerBaseTest {
    @Test
    public void testProxyHandlesRedirects() throws Exception {
        var namespaceName = NamespaceName.get("public", BrokerTestUtil.newUniqueName("redirecttest"));
        admin.namespaces().createNamespace(namespaceName.toString(), 16);

        @Cleanup("stop")
        HttpClient client = new HttpClient();
        client.setFollowRedirects(false);
        client.start();

        for (int i = 0; i < 100; i++) {
            var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName, "topic-" + i);
            ContentResponse response =
                    client.newRequest(getProxyHttpServiceUrl() + "/lookup/v2/topic/" + topicName.getLookupName())
                            .followRedirects(false).send();
            assertThat(response.getStatus()).isEqualTo(200);
        }
    }

    @Test
    public void testProxyHandlesReplayingContent() throws Exception {
        var namespaceName = NamespaceName.get("public", BrokerTestUtil.newUniqueName("replaytest"));
        admin.namespaces().createNamespace(namespaceName.toString(), 16);

        @Cleanup("stop")
        HttpClient client = new HttpClient();
        client.setFollowRedirects(false);
        client.start();

        for (int i = 0; i < 100; i++) {
            var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName, "topic-" + i);
            proxiedAdmin.topics().createNonPartitionedTopic(topicName.toString(), Map.of("index", String.valueOf(i)));
            DelayedDeliveryPolicies delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                    .active(true)
                    .tickTime(1234)
                    .maxDeliveryDelayInMillis(600000)
                    .build();
            proxiedAdmin.topicPolicies().setDelayedDeliveryPolicy(topicName.toString(), delayedDeliveryPolicies);

            DelayedDeliveryPolicies updatedPolicy =
                    proxiedAdmin.topicPolicies().getDelayedDeliveryPolicy(topicName.toString());

            assertThat(updatedPolicy).isEqualTo(delayedDeliveryPolicies);

            Map<String, String> topicProperties = proxiedAdmin.topics().getProperties(topicName.toString());
            assertThat(topicProperties).containsEntry("index", String.valueOf(i));
        }
    }
}
