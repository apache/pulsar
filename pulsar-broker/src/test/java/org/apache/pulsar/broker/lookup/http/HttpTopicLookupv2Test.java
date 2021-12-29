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
package org.apache.pulsar.broker.lookup.http;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.lookup.NamespaceData;
import org.apache.pulsar.broker.lookup.RedirectData;
import org.apache.pulsar.broker.lookup.v1.TopicLookup;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * HTTP lookup unit tests.
 */
@Test(groups = "broker")
public class HttpTopicLookupv2Test {

    private PulsarService pulsar;
    private NamespaceService ns;
    private AuthorizationService auth;
    private ServiceConfiguration config;
    private Set<String> clusters;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setUp() throws Exception {
        pulsar = mock(PulsarService.class);
        ns = mock(NamespaceService.class);
        auth = mock(AuthorizationService.class);
        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        clusters = new TreeSet<>();
        clusters.add("use");
        clusters.add("usc");
        clusters.add("usw");
        ClusterData useData = ClusterData.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build();
        ClusterData uscData = ClusterData.builder().serviceUrl("http://broker.messaging.usc.example.com:8080").build();
        ClusterData uswData = ClusterData.builder().serviceUrl("http://broker.messaging.usw.example.com:8080").build();
        doReturn(config).when(pulsar).getConfiguration();

        ClusterResources clusters = mock(ClusterResources.class);
        when(clusters.getClusterAsync("use")).thenReturn(CompletableFuture.completedFuture(Optional.of(useData)));
        when(clusters.getClusterAsync("usc")).thenReturn(CompletableFuture.completedFuture(Optional.of(uscData)));
        when(clusters.getClusterAsync("usw")).thenReturn(CompletableFuture.completedFuture(Optional.of(uswData)));
        PulsarResources resources = mock(PulsarResources.class);
        when(resources.getClusterResources()).thenReturn(clusters);
        when(pulsar.getPulsarResources()).thenReturn(resources);

        doReturn(ns).when(pulsar).getNamespaceService();
        BrokerService brokerService = mock(BrokerService.class);
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(auth).when(brokerService).getAuthorizationService();
        doReturn(new Semaphore(1000)).when(brokerService).getLookupRequestSemaphore();
    }

    @Test
    public void crossColoLookup() throws Exception {

        TopicLookup destLookup = spy(new TopicLookup());
        doReturn(false).when(destLookup).isRequestHttps();
        destLookup.setPulsar(pulsar);
        doReturn("null").when(destLookup).clientAppId();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        UriInfo uriInfo = mock(UriInfo.class);
        uriField.set(destLookup, uriInfo);
        URI uri = URI.create("http://localhost:8080/lookup/v2/destination/topic/myprop/usc/ns2/topic1");
        doReturn(uri).when(uriInfo).getRequestUri();
        doReturn(true).when(config).isAuthorizationEnabled();

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        destLookup.lookupTopicAsync(TopicDomain.persistent.value(), "myprop", "usc", "ns2", "topic1", false,
                asyncResponse, null, null);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), WebApplicationException.class);
        WebApplicationException wae = (WebApplicationException) arg.getValue();
        assertEquals(wae.getResponse().getStatus(), Status.TEMPORARY_REDIRECT.getStatusCode());
    }

    @Test
    public void testLookupTopicNotExist() throws Exception {

        MockTopicLookup destLookup = spy(new MockTopicLookup());
        doReturn(false).when(destLookup).isRequestHttps();
        destLookup.setPulsar(pulsar);
        doReturn("null").when(destLookup).clientAppId();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        UriInfo uriInfo = mock(UriInfo.class);
        uriField.set(destLookup, uriInfo);
        URI uri = URI.create("http://localhost:8080/lookup/v2/destination/topic/myprop/usc/ns2/topic1");
        doReturn(uri).when(uriInfo).getRequestUri();
        doReturn(true).when(config).isAuthorizationEnabled();

        NamespaceService namespaceService = pulsar.getNamespaceService();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        future.complete(false);
        doReturn(future).when(namespaceService).checkTopicExists(any(TopicName.class));

        AsyncResponse asyncResponse1 = mock(AsyncResponse.class);
        destLookup.lookupTopicAsync(TopicDomain.persistent.value(), "myprop", "usc", "ns2", "topic_not_exist", false,
                asyncResponse1, null, null);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse1).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), RestException.class);
        RestException restException = (RestException) arg.getValue();
        assertEquals(restException.getResponse().getStatus(), Status.NOT_FOUND.getStatusCode());
    }

    static class MockTopicLookup extends TopicLookup {
        @Override
        protected void validateClusterOwnership(String s) {
            // do nothing
        }
    }
    
    @Test
    public void testNotEnoughLookupPermits() throws Exception {

        BrokerService brokerService = pulsar.getBrokerService();
        doReturn(new Semaphore(0)).when(brokerService).getLookupRequestSemaphore();

        TopicLookup destLookup = spy(new TopicLookup());
        doReturn(false).when(destLookup).isRequestHttps();
        destLookup.setPulsar(pulsar);
        doReturn("null").when(destLookup).clientAppId();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        UriInfo uriInfo = mock(UriInfo.class);
        uriField.set(destLookup, uriInfo);
        URI uri = URI.create("http://localhost:8080/lookup/v2/destination/topic/myprop/usc/ns2/topic1");
        doReturn(uri).when(uriInfo).getRequestUri();
        doReturn(true).when(config).isAuthorizationEnabled();

        AsyncResponse asyncResponse1 = mock(AsyncResponse.class);
        destLookup.lookupTopicAsync(TopicDomain.persistent.value(), "myprop", "usc", "ns2", "topic1", false,
                asyncResponse1, null, null);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse1).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), WebApplicationException.class);
        WebApplicationException wae = (WebApplicationException) arg.getValue();
        assertEquals(wae.getResponse().getStatus(), Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    public void testValidateReplicationSettingsOnNamespace() throws Exception {

        final String property = "my-prop";
        final String cluster = "global";
        final String ns1 = "ns1";
        final String ns2 = "ns2";
        Policies policies1 = new Policies();
//        doReturn(Optional.of(policies1)).when(policiesCache)
//                .get(AdminResource.path(POLICIES, property, cluster, ns1));
        Policies policies2 = new Policies();
        policies2.replication_clusters = Sets.newHashSet("invalid-localCluster");
//        doReturn(Optional.of(policies2)).when(policiesCache)
//                .get(AdminResource.path(POLICIES, property, cluster, ns2));

        TopicLookup destLookup = spy(new TopicLookup());
        doReturn(false).when(destLookup).isRequestHttps();
        destLookup.setPulsar(pulsar);
        doReturn("null").when(destLookup).clientAppId();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        UriInfo uriInfo = mock(UriInfo.class);
        uriField.set(destLookup, uriInfo);
        doReturn(false).when(config).isAuthorizationEnabled();

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        destLookup.lookupTopicAsync(TopicDomain.persistent.value(), property, cluster, ns1, "empty-cluster",
                false, asyncResponse, null, null);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), RestException.class);

        AsyncResponse asyncResponse2 = mock(AsyncResponse.class);
        destLookup.lookupTopicAsync(TopicDomain.persistent.value(), property, cluster, ns2,
                "invalid-localCluster", false, asyncResponse2, null, null);
        ArgumentCaptor<Throwable> arg2 = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse2).resume(arg2.capture());

        // Should have raised exception for invalid cluster
        assertEquals(arg2.getValue().getClass(), RestException.class);
    }

    @Test
    public void testDataPojo() {
        final String url = "localhost:8080";
        NamespaceData data1 = new NamespaceData(url);
        assertEquals(data1.getBrokerUrl(), url);
        RedirectData data2 = new RedirectData(url);
        assertEquals(data2.getRedirectLookupAddress(), url);
    }

}
