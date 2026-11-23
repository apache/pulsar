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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.broker.rest.Topics;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * REST produce tests that replace the broker's services or start a second broker, so each one runs on a broker of
 * its own. {@link TopicsTest} has the tests that share one.
 */
public class TopicsOwnBrokerTest extends MockedPulsarServiceBaseTest {

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
        HttpServletRequest request = mock(HttpServletRequest.class);
        doReturn("http").when(request).getScheme();
        topics.setHttpRequest(request);
        doReturn(TopicDomain.persistent.value()).when(topics).domain();
        doReturn("test-app").when(topics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(topics).clientAuthData();
        doReturn(null).when(topics).getWebServiceListenerName();
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Set.of("role1", "role2"),
                Set.of(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                                           Set.of(testLocalCluster));
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProduceFailed() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/" + testNamespace
                + "/" + testTopicName);
        pulsar.getBrokerService().getTopic("persistent://" + testTenant + "/" + testNamespace
                + "/" + testTopicName, false).thenAccept(topic -> {
            try {
                PersistentTopic mockPersistentTopic = spy((PersistentTopic) topic.get());
                AtomicInteger count = new AtomicInteger();
                doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Topic.PublishContext publishContext = invocationOnMock.getArgument(1);
                        if (count.getAndIncrement() < 2) {
                            publishContext.completed(null, -1, -1);
                        } else {
                            publishContext.completed(new BrokerServiceException.TopicFencedException("Fake exception"),
                                    -1, -1);
                        }
                        return null;
                    }
                }).when(mockPersistentTopic).publishMessage(any(), any());
                BrokerService mockBrokerService = spy(pulsar.getBrokerService());
                doReturn(CompletableFuture.completedFuture(Optional.of(mockPersistentTopic)))
                        .when(mockBrokerService).getTopic(anyString(), anyBoolean());
                doReturn(mockBrokerService).when(pulsar).getBrokerService();
                AsyncResponse asyncResponse = mock(AsyncResponse.class);
                Schema<String> schema = StringSchema.utf8();
                ProducerMessages producerMessages = new ProducerMessages();
                producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                        writeValueAsString(schema.getSchemaInfo()));
                producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                        writeValueAsString(schema.getSchemaInfo()));
                String message = "["
                        + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\","
                        + "\"eventTime\":1603045262772,\"sequenceId\":1},"
                        + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\","
                        + "\"eventTime\":1603045262772,\"sequenceId\":2},"
                        + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\","
                        + "\"eventTime\":1603045262772,\"sequenceId\":3},"
                        + "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\","
                        + "\"eventTime\":1603045262772,\"sequenceId\":4}]";
                producerMessages.setMessages(TopicsTest.createMessages(message));
                // Previous request should trigger namespace bundle loading, retry produce.
                topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                        false, producerMessages);
                ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
                verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
                Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
                Object responseEntity = responseCaptor.getValue().getEntity();
                Assert.assertTrue(responseEntity instanceof ProducerAcks);
                ProducerAcks response = (ProducerAcks) responseEntity;
                Assert.assertEquals(response.getMessagePublishResults().size(), 4);
                int errorResponse = 0;
                for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
                    int errorCode = response.getMessagePublishResults().get(index).getErrorCode();
                    if (0 == errorCode) {
                        Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                                .getMessageId().split(":")[2]), -1);
                        Assert.assertTrue(response.getMessagePublishResults().get(index)
                                .getMessageId().length() > 0);
                    } else {
                        errorResponse++;
                        Assert.assertEquals(errorCode, 2);
                        Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorMsg(),
                                "org.apache.pulsar.broker.service.BrokerServiceException$"
                                         + "TopicFencedException: Fake exception");
                    }
                }
                // Add entry start to fail after 2nd operation, we published 4 msg so expecting 2 error response.
                Assert.assertTrue(errorResponse == 2);
            } catch (Throwable e) {
                Assert.fail(e.getMessage());
            }
        }).get();
    }

    @Test
    public void testLookUpWithRedirect() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        URI requestPath = URI.create(pulsar.getWebServiceAddress() + "/topics/my-tenant/my-namespace/my-topic");
        //create topic on one broker
        admin.topics().createNonPartitionedTopic(topicName);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
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
        producerMessages.setMessages(TopicsTest.createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        // Verify got redirect response
        Assert.assertEquals(responseCaptor.getValue().getStatusInfo(), Response.Status.TEMPORARY_REDIRECT);
        // Verify URI point to address of broker the topic was created on
        Assert.assertEquals(responseCaptor.getValue().getLocation().toString(), requestPath.toString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookUpWithException() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        NamespaceService nameSpaceService = mock(NamespaceService.class);
        CompletableFuture future = new CompletableFuture();
        future.completeExceptionally(new BrokerServiceException("Fake Exception"));
        CompletableFuture existFuture = new CompletableFuture();
        existFuture.complete(TopicExistsInfo.newNonPartitionedTopicExists());
        doReturn(future).when(nameSpaceService).getBrokerServiceUrlAsync(any(), any());
        doReturn(existFuture).when(nameSpaceService).checkTopicExistsAsync(any());
        CompletableFuture existBooleanFuture = new CompletableFuture();
        existBooleanFuture.complete(false);
        doReturn(existBooleanFuture).when(nameSpaceService).checkNonPartitionedTopicExists(any());
        doReturn(nameSpaceService).when(pulsar).getNamespaceService();
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(TopicsTest.createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertTrue(responseCaptor.getValue().getMessage().contains(topicName + " not found"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookUpTopicNotExist() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        NamespaceService nameSpaceService = mock(NamespaceService.class);
        CompletableFuture existFuture = new CompletableFuture();
        existFuture.complete(TopicExistsInfo.newTopicNotExists());
        CompletableFuture existBooleanFuture = new CompletableFuture();
        existBooleanFuture.complete(false);
        doReturn(existFuture).when(nameSpaceService).checkTopicExistsAsync(any());
        doReturn(existBooleanFuture).when(nameSpaceService).checkNonPartitionedTopicExists(any());
        doReturn(nameSpaceService).when(pulsar).getNamespaceService();
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(TopicsTest.createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        System.out.println(responseCaptor.getValue().getMessage());
        Assert.assertTrue(responseCaptor.getValue().getMessage()
                .contains(String.format("Topic %s not found", topicName)));
    }
}
