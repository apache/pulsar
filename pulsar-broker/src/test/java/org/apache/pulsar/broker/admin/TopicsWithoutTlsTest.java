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
package org.apache.pulsar.broker.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Cleanup;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.rest.Topics;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class TopicsWithoutTlsTest extends MockedPulsarServiceBaseTest {

    private Topics topics;
    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        topics = spy(new Topics());
        topics.setPulsar(pulsar);
        doReturn(TopicDomain.persistent.value()).when(topics).domain();
        doReturn("test-app").when(topics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(topics).clientAuthData();
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Set.of("role1", "role2"), Set.of(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                                           Set.of(testLocalCluster));
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setBrokerServicePortTls(Optional.empty());
        this.conf.setWebServicePortTls(Optional.empty());
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testLookUpWithRedirect() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        URI requestPath = URI.create(pulsar.getWebServiceAddress() + "/topics/my-tenant/my-namespace/my-topic");
        //create topic on one broker
        admin.topics().createNonPartitionedTopic(topicName);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        @Cleanup
        PulsarTestContext pulsarTestContext2 = createAdditionalPulsarTestContext(conf);
        PulsarService pulsar2 = pulsarTestContext2.getPulsarService();
        doReturn(false).when(topics).isRequestHttps();
        UriInfo uriInfo = mock(UriInfo.class);
        doReturn(requestPath).when(uriInfo).getRequestUri();
        FieldUtils.writeField(topics, "uri", uriInfo, true);
        //do produce on another broker
        topics.setPulsar(pulsar2);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        // Verify got redirect response
        Assert.assertEquals(responseCaptor.getValue().getStatusInfo(), Response.Status.TEMPORARY_REDIRECT);
        // Verify URI point to address of broker the topic was created on
        Assert.assertEquals(responseCaptor.getValue().getLocation().toString(), requestPath.toString());
    }

    private static List<ProducerMessage> createMessages(String message) throws JsonProcessingException {
        return ObjectMapperFactory.getMapper().reader()
                .forType(new TypeReference<List<ProducerMessage>>() {
                }).readValue(message);
    }
}
