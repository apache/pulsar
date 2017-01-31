/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.lookup.http;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.authorization.AuthorizationManager;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.broker.lookup.DestinationLookup;
import com.yahoo.pulsar.broker.lookup.NamespaceData;
import com.yahoo.pulsar.broker.lookup.RedirectData;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.broker.web.RestException;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

/**
 * HTTP lookup unit tests.
 *
 *
 */
public class HttpDestinationLookupv2Test {

    private PulsarService pulsar;
    private NamespaceService ns;
    private AuthorizationManager auth;
    private ServiceConfiguration config;
    private ConfigurationCacheService mockConfigCache;
    private ZooKeeperChildrenCache clustersListCache;
    private ZooKeeperDataCache<ClusterData> clustersCache;
    private ZooKeeperDataCache<Policies> policiesCache;
    private Set<String> clusters;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setUp() throws Exception {
        pulsar = mock(PulsarService.class);
        ns = mock(NamespaceService.class);
        auth = mock(AuthorizationManager.class);
        mockConfigCache = mock(ConfigurationCacheService.class);
        clustersListCache = mock(ZooKeeperChildrenCache.class);
        clustersCache = mock(ZooKeeperDataCache.class);
        policiesCache = mock(ZooKeeperDataCache.class);
        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        clusters = new TreeSet<String>();
        clusters.add("use");
        clusters.add("usc");
        clusters.add("usw");
        ClusterData useData = new ClusterData("http://broker.messaging.use.example.com:8080");
        ClusterData uscData = new ClusterData("http://broker.messaging.usc.example.com:8080");
        ClusterData uswData = new ClusterData("http://broker.messaging.usw.example.com:8080");
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(mockConfigCache).when(pulsar).getConfigurationCache();
        doReturn(clustersListCache).when(mockConfigCache).clustersListCache();
        doReturn(clustersCache).when(mockConfigCache).clustersCache();
        doReturn(policiesCache).when(mockConfigCache).policiesCache();
        doReturn(Optional.of(useData)).when(clustersCache).get(AdminResource.path("clusters", "use"));
        doReturn(Optional.of(uscData)).when(clustersCache).get(AdminResource.path("clusters", "usc"));
        doReturn(Optional.of(uswData)).when(clustersCache).get(AdminResource.path("clusters", "usw"));
        doReturn(CompletableFuture.completedFuture(Optional.of(useData))).when(clustersCache).getAsync(AdminResource.path("clusters", "use"));
        doReturn(CompletableFuture.completedFuture(Optional.of(uscData))).when(clustersCache).getAsync(AdminResource.path("clusters", "usc"));
        doReturn(CompletableFuture.completedFuture(Optional.of(uswData))).when(clustersCache).getAsync(AdminResource.path("clusters", "usw"));
        doReturn(clusters).when(clustersListCache).get();
        doReturn(ns).when(pulsar).getNamespaceService();
        BrokerService brokerService = mock(BrokerService.class);
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(auth).when(brokerService).getAuthorizationManager();
        doReturn(new Semaphore(1000)).when(brokerService).getLookupRequestSemaphore();
    }

    @Test
    public void crossColoLookup() throws Exception {

        DestinationLookup destLookup = spy(new DestinationLookup());
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
        destLookup.lookupDestinationAsync("myprop", "usc", "ns2", "topic1", false, asyncResponse);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), WebApplicationException.class);
        WebApplicationException wae = (WebApplicationException) arg.getValue();
        assertEquals(wae.getResponse().getStatus(), Status.TEMPORARY_REDIRECT.getStatusCode());
    }
    
    
    @Test
    public void testNotEnoughLookupPermits() throws Exception {

        BrokerService brokerService = pulsar.getBrokerService();
        doReturn(new Semaphore(0)).when(brokerService).getLookupRequestSemaphore();

        DestinationLookup destLookup = spy(new DestinationLookup());
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
        destLookup.lookupDestinationAsync("myprop", "usc", "ns2", "topic1", false, asyncResponse1);

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
        doReturn(Optional.of(policies1)).when(policiesCache)
                .get(AdminResource.path("policies", property, cluster, ns1));
        Policies policies2 = new Policies();
        policies2.replication_clusters = Lists.newArrayList("invalid-localCluster");
        doReturn(Optional.of(policies2)).when(policiesCache)
                .get(AdminResource.path("policies", property, cluster, ns2));

        DestinationLookup destLookup = spy(new DestinationLookup());
        doReturn(false).when(destLookup).isRequestHttps();
        destLookup.setPulsar(pulsar);
        doReturn("null").when(destLookup).clientAppId();
        Field uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        UriInfo uriInfo = mock(UriInfo.class);
        uriField.set(destLookup, uriInfo);
        doReturn(false).when(config).isAuthorizationEnabled();

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        destLookup.lookupDestinationAsync(property, cluster, ns1, "empty-cluster", false, asyncResponse);

        ArgumentCaptor<Throwable> arg = ArgumentCaptor.forClass(Throwable.class);
        verify(asyncResponse).resume(arg.capture());
        assertEquals(arg.getValue().getClass(), RestException.class);

        AsyncResponse asyncResponse2 = mock(AsyncResponse.class);
        destLookup.lookupDestinationAsync(property, cluster, ns2, "invalid-localCluster", false, asyncResponse2);
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
