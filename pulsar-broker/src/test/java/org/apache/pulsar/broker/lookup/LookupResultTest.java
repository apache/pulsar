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

package org.apache.pulsar.broker.lookup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LookupResultTest {

    private static final String BROKER_ID = "broker-1:8080";
    private static final String WEB_URL = "http://broker-1:8080";
    private static final String WEB_URL_TLS = "https://broker-1:8443";
    private static final String PULSAR_URL = "pulsar://broker-1:6650";
    private static final String PULSAR_URL_TLS = "pulsar+ssl://broker-1:6651";

    private static final String LISTENER_NAME = "internal";
    private static final String LISTENER_PULSAR_URL = "pulsar://gateway:7000";
    private static final String LISTENER_PULSAR_URL_TLS = "pulsar+ssl://gateway:7001";
    private static final String LISTENER_HTTP_URL = "http://gateway:8000";
    private static final String LISTENER_HTTP_URL_TLS = "https://gateway:8001";

    private static Map<String, AdvertisedListener> singleListener() {
        Map<String, AdvertisedListener> map = new HashMap<>();
        map.put(LISTENER_NAME, AdvertisedListener.builder()
                .brokerServiceUrl(URI.create(LISTENER_PULSAR_URL))
                .brokerServiceUrlTls(URI.create(LISTENER_PULSAR_URL_TLS))
                .brokerHttpUrl(URI.create(LISTENER_HTTP_URL))
                .brokerHttpsUrl(URI.create(LISTENER_HTTP_URL_TLS))
                .build());
        return map;
    }

    private static BrokerLookupData newBrokerLookupData(Map<String, AdvertisedListener> listeners) {
        return new BrokerLookupData(BROKER_ID, WEB_URL, WEB_URL_TLS, PULSAR_URL, PULSAR_URL_TLS,
                listeners, Collections.emptyMap(), true, true,
                "ModularLoadManager", 0L, "3.0", Collections.emptyMap());
    }

    @Test
    public void testTypeBrokerUrl() {
        LookupResult result = LookupResult.builder().type(LookupResult.Type.BrokerUrl).build();
        assertTrue(result.isBrokerUrl());
        assertFalse(result.isRedirect());
        assertFalse(result.isLoadManagerMigration());
    }

    @Test
    public void testTypeRedirectUrl() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .authoritativeRedirect(true)
                .build();
        assertFalse(result.isBrokerUrl());
        assertTrue(result.isRedirect());
        assertFalse(result.isLoadManagerMigration());
        assertTrue(result.isAuthoritativeRedirect());
    }

    @Test
    public void testTypeLoadManagerMigrationUrl() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.LoadManagerMigrationUrl)
                .build();
        assertFalse(result.isBrokerUrl());
        assertTrue(result.isRedirect());
        assertTrue(result.isLoadManagerMigration());
        assertFalse(result.isAuthoritativeRedirect());
    }

    @Test
    public void testTypeIsRequiredByBuilder() {
        assertThrows(NullPointerException.class, () -> LookupResult.builder().build());
    }

    @Test
    public void testCreateFromBrokerLookupDataWithoutOptions() {
        LookupResult result = LookupResult.create(newBrokerLookupData(Collections.emptyMap()), null);
        assertTrue(result.isBrokerUrl());
        assertFalse(result.isRedirect());
        assertEquals(result.getLookupData().getBrokerId(), BROKER_ID);
        assertEquals(result.getLookupData().getHttpUrl(), WEB_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), WEB_URL_TLS);
        assertEquals(result.getLookupData().getBrokerUrl(), PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), PULSAR_URL_TLS);
        assertNull(result.getBrokerServiceListenerName());
        assertNull(result.getWebServiceListenerName());
    }

    @Test
    public void testCreateAsRedirectFromBrokerLookupData() {
        LookupResult result = LookupResult.create(newBrokerLookupData(Collections.emptyMap()),
                LookupOptions.builder().build(), true);
        assertTrue(result.isRedirect());
        assertFalse(result.isLoadManagerMigration());
        assertTrue(result.isAuthoritativeRedirect());
    }

    @Test
    public void testCreateLoadManagerMigrationLookupResult() {
        LookupResult result = LookupResult.createLoadManagerMigrationLookupResult(
                newBrokerLookupData(Collections.emptyMap()), LookupOptions.builder().build());
        assertTrue(result.isRedirect());
        assertTrue(result.isLoadManagerMigration());
        assertFalse(result.isAuthoritativeRedirect());
    }

    @Test
    public void testCreateWithAdvertisedListenerOverridesBrokerServiceUrls() {
        BrokerLookupData lookupData = newBrokerLookupData(singleListener());
        LookupOptions options = LookupOptions.builder().advertisedListenerName(LISTENER_NAME).build();

        LookupResult result = LookupResult.create(lookupData, options);

        assertEquals(result.getBrokerServiceListenerName(), LISTENER_NAME);
        assertNull(result.getWebServiceListenerName());
        assertEquals(result.getLookupData().getBrokerUrl(), LISTENER_PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), LISTENER_PULSAR_URL_TLS);
        // web URLs are untouched when only advertisedListenerName is set
        assertEquals(result.getLookupData().getHttpUrl(), WEB_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), WEB_URL_TLS);
    }

    @Test
    public void testCreateWithUnknownAdvertisedListenerLeavesUrlsUnchanged() {
        BrokerLookupData lookupData = newBrokerLookupData(singleListener());
        LookupOptions options = LookupOptions.builder().advertisedListenerName("does-not-exist").build();

        LookupResult result = LookupResult.create(lookupData, options);

        assertNull(result.getBrokerServiceListenerName());
        assertNull(result.getWebServiceListenerName());
        assertEquals(result.getLookupData().getBrokerUrl(), PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), PULSAR_URL_TLS);
        assertEquals(result.getLookupData().getHttpUrl(), WEB_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), WEB_URL_TLS);
    }

    @Test
    public void testCreateWithWebServiceAdvertisedListenerOverridesHttpUrls() {
        BrokerLookupData lookupData = newBrokerLookupData(singleListener());
        LookupOptions options = LookupOptions.builder()
                .webServiceAdvertisedListenerName(LISTENER_NAME)
                .build();

        LookupResult result = LookupResult.create(lookupData, options);

        assertEquals(result.getWebServiceListenerName(), LISTENER_NAME);
        // Since the listener has broker service URLs configured, brokerServiceListenerName
        // defaults to the same listener name (per LookupResult contract).
        assertEquals(result.getBrokerServiceListenerName(), LISTENER_NAME);
        assertEquals(result.getLookupData().getHttpUrl(), LISTENER_HTTP_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), LISTENER_HTTP_URL_TLS);
        assertEquals(result.getLookupData().getBrokerUrl(), LISTENER_PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), LISTENER_PULSAR_URL_TLS);
    }

    @Test
    public void testCreateWithWebServiceListenerWithoutBrokerServiceUrls() {
        Map<String, AdvertisedListener> listeners = new HashMap<>();
        listeners.put(LISTENER_NAME, AdvertisedListener.builder()
                .brokerHttpUrl(URI.create(LISTENER_HTTP_URL))
                .brokerHttpsUrl(URI.create(LISTENER_HTTP_URL_TLS))
                .build());
        BrokerLookupData lookupData = newBrokerLookupData(listeners);
        LookupOptions options = LookupOptions.builder()
                .webServiceAdvertisedListenerName(LISTENER_NAME)
                .build();

        LookupResult result = LookupResult.create(lookupData, options);

        assertEquals(result.getWebServiceListenerName(), LISTENER_NAME);
        // No broker service URLs on the listener — broker URLs stay on the broker's defaults
        // and brokerServiceListenerName is not overridden.
        assertNull(result.getBrokerServiceListenerName());
        assertEquals(result.getLookupData().getBrokerUrl(), PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), PULSAR_URL_TLS);
        assertEquals(result.getLookupData().getHttpUrl(), LISTENER_HTTP_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), LISTENER_HTTP_URL_TLS);
    }

    @Test
    public void testCreateWithBothListenerNamesIndependently() {
        Map<String, AdvertisedListener> listeners = new HashMap<>();
        listeners.put("broker-listener", AdvertisedListener.builder()
                .brokerServiceUrl(URI.create("pulsar://broker-listener:7000"))
                .build());
        listeners.put("web-listener", AdvertisedListener.builder()
                .brokerHttpUrl(URI.create("http://web-listener:8000"))
                .build());
        BrokerLookupData lookupData = newBrokerLookupData(listeners);
        LookupOptions options = LookupOptions.builder()
                .advertisedListenerName("broker-listener")
                .webServiceAdvertisedListenerName("web-listener")
                .build();

        LookupResult result = LookupResult.create(lookupData, options);

        assertEquals(result.getBrokerServiceListenerName(), "broker-listener");
        assertEquals(result.getWebServiceListenerName(), "web-listener");
        assertEquals(result.getLookupData().getBrokerUrl(), "pulsar://broker-listener:7000");
        assertNull(result.getLookupData().getBrokerUrlTls());
        assertEquals(result.getLookupData().getHttpUrl(), "http://web-listener:8000");
        assertNull(result.getLookupData().getHttpUrlTls());
    }

    @Test
    public void testCreateFromLocalBrokerData() {
        LocalBrokerData data = new LocalBrokerData(BROKER_ID, WEB_URL, WEB_URL_TLS, PULSAR_URL, PULSAR_URL_TLS,
                singleListener());

        LookupResult result = LookupResult.create(data, LookupOptions.builder()
                .advertisedListenerName(LISTENER_NAME).build());
        assertTrue(result.isBrokerUrl());
        assertEquals(result.getLookupData().getBrokerId(), BROKER_ID);
        assertEquals(result.getBrokerServiceListenerName(), LISTENER_NAME);
        assertEquals(result.getLookupData().getBrokerUrl(), LISTENER_PULSAR_URL);
    }

    @Test
    public void testCreateAsRedirectFromLocalBrokerData() {
        LocalBrokerData data = new LocalBrokerData(BROKER_ID, WEB_URL, WEB_URL_TLS, PULSAR_URL, PULSAR_URL_TLS);

        LookupResult result = LookupResult.create(data, LookupOptions.builder().build(), true);
        assertTrue(result.isRedirect());
        assertTrue(result.isAuthoritativeRedirect());
    }

    @Test
    public void testCreateFromNamespaceEphemeralData() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData(BROKER_ID, PULSAR_URL, PULSAR_URL_TLS,
                WEB_URL, WEB_URL_TLS, false, singleListener());

        LookupResult result = LookupResult.create(nsData, LookupOptions.builder().build());
        assertTrue(result.isBrokerUrl());
        assertEquals(result.getLookupData().getBrokerId(), BROKER_ID);
        assertEquals(result.getLookupData().getBrokerUrl(), PULSAR_URL);
        assertEquals(result.getLookupData().getBrokerUrlTls(), PULSAR_URL_TLS);
        assertEquals(result.getLookupData().getHttpUrl(), WEB_URL);
        assertEquals(result.getLookupData().getHttpUrlTls(), WEB_URL_TLS);
    }

    @Test
    public void testCreateAsRedirectFromNamespaceEphemeralData() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData(BROKER_ID, PULSAR_URL, PULSAR_URL_TLS,
                WEB_URL, WEB_URL_TLS, false);
        LookupResult result = LookupResult.create(nsData, LookupOptions.builder().build(), true);
        assertTrue(result.isRedirect());
        assertTrue(result.isAuthoritativeRedirect());
    }

    @Test
    public void testBackwardCompatibleBrokerIdDerivationFromWebServiceUrl() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData(null, PULSAR_URL, PULSAR_URL_TLS,
                WEB_URL, WEB_URL_TLS, false);
        LookupResult result = LookupResult.create(nsData, LookupOptions.builder().build());
        assertEquals(result.getLookupData().getBrokerId(), "broker-1:8080");
    }

    @Test
    public void testBackwardCompatibleBrokerIdDerivationFromWebServiceUrlTls() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData(null, PULSAR_URL, PULSAR_URL_TLS,
                null, WEB_URL_TLS, false);
        LookupResult result = LookupResult.create(nsData, LookupOptions.builder().build());
        assertEquals(result.getLookupData().getBrokerId(), "broker-1:8443");
    }

    @Test
    public void testBackwardCompatibleBrokerIdRemainsNullWhenNoWebUrls() {
        NamespaceEphemeralData nsData = new NamespaceEphemeralData(null, PULSAR_URL, PULSAR_URL_TLS,
                null, null, false);
        LookupResult result = LookupResult.create(nsData, LookupOptions.builder().build());
        assertNull(result.getLookupData().getBrokerId());
    }

    @Test
    public void testToRedirectUriUsesLookupHostAndPortPreservesPath() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .httpUrlTls(WEB_URL_TLS)
                .authoritativeRedirect(true)
                .build();

        URI request = URI.create("http://original-host:1234/admin/v2/persistent/public/default/topic?x=1");

        URI redirect = result.toRedirectUri(request);

        assertEquals(redirect.getScheme(), "http");
        assertEquals(redirect.getHost(), "broker-1");
        assertEquals(redirect.getPort(), 8080);
        assertEquals(redirect.getPath(), "/admin/v2/persistent/public/default/topic");
        assertTrue(redirect.getQuery().contains("x=1"));
        // authoritative parameter is taken from the LookupResult, not from the request URI
        assertTrue(redirect.getQuery().contains("authoritative=true"));
    }

    @Test
    public void testToRedirectUriUsesAuthoritativeFromLookupResultNotRequest() {
        // The redirect should reflect the LookupResult's authoritativeRedirect flag, even when
        // the incoming request URI carried a different authoritative value.
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .authoritativeRedirect(true)
                .build();

        URI request = URI.create("http://original-host:1234/admin?authoritative=false");

        URI redirect = result.toRedirectUri(request);

        assertTrue(redirect.getQuery().contains("authoritative=true"));
    }

    @Test
    public void testToRedirectUriHttpsUsesTlsHostAndPort() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .httpUrlTls(WEB_URL_TLS)
                .build();

        URI request = URI.create("https://original-host:1234/admin/v2/topic");

        URI redirect = result.toRedirectUri(request);

        assertEquals(redirect.getScheme(), "https");
        assertEquals(redirect.getHost(), "broker-1");
        assertEquals(redirect.getPort(), 8443);
    }

    @Test
    public void testToRedirectUriHttpsRequiresTlsUrl() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .brokerId(BROKER_ID)
                .httpUrl(WEB_URL)
                .build();

        URI request = URI.create("https://original-host:1234/admin");

        WebApplicationException e = expectThrows(WebApplicationException.class, () -> result.toRedirectUri(request));
        assertEquals(e.getResponse().getStatus(), Response.Status.PRECONDITION_FAILED.getStatusCode());
    }

    @Test
    public void testToRedirectUriRemovesAuthoritativeWhenBrokerUrlType() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.BrokerUrl)
                .httpUrl(WEB_URL)
                .build();

        URI request = URI.create("http://original-host:1234/admin?authoritative=true&keep=me");

        URI redirect = result.toRedirectUri(request);

        String query = redirect.getQuery();
        assertNotNull(query);
        assertFalse(query.contains("authoritative"));
        assertTrue(query.contains("keep=me"));
    }

    @Test
    public void testToRedirectUriPassesAuthoritativeWhenLoadManagerMigration() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.LoadManagerMigrationUrl)
                .httpUrl(WEB_URL)
                .authoritativeRedirect(true)
                .build();

        URI request = URI.create("http://original-host:1234/admin");

        URI redirect = result.toRedirectUri(request);

        assertTrue(redirect.getQuery().contains("authoritative=true"));
    }

    @Test
    public void testToRedirectUriWithExplicitAuthoritativeOverride() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .authoritativeRedirect(true)
                .build();

        URI request = URI.create("http://original-host:1234/admin");

        // explicit override wins over the LookupResult's authoritativeRedirect
        URI redirect = result.toRedirectUri(request, false);

        assertTrue(redirect.getQuery().contains("authoritative=false"));
    }

    @Test
    public void testToRedirectUriPreservesExistingQueryParameters() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .build();

        URI request = URI.create("http://original-host:1234/admin?listenerName=internal&other=1");

        URI redirect = result.toRedirectUri(request);

        String query = redirect.getQuery();
        assertNotNull(query);
        assertTrue(query.contains("listenerName=internal"));
        assertTrue(query.contains("other=1"));
    }

    @Test
    public void testToLookupRedirectUriInjectsListenerNameWhenResolved() {
        // Topic-lookup case: the original request may carry the listener name in a header that the
        // redirect does not preserve. toLookupRedirectUri must inject it as a query parameter so the
        // next broker sees it.
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .brokerServiceListenerName("external")
                .build();

        URI request = URI.create("http://original-host:1234/lookup/v2/topic/persistent/p/d/t");

        URI redirect = result.toLookupRedirectUri(request);

        String query = redirect.getQuery();
        assertNotNull(query);
        assertTrue(query.contains("listenerName=external"), "query=" + query);
    }

    @Test
    public void testToRedirectUriDoesNotInjectListenerNameForAdminPaths() {
        // Admin redirects do not understand the listenerName query parameter — even if the
        // LookupResult has a resolved brokerServiceListenerName, toRedirectUri must leave the
        // request's query string alone.
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .brokerServiceListenerName("external")
                .build();

        URI request = URI.create("http://original-host:1234/admin/v2/persistent/p/d/t");

        URI redirect = result.toRedirectUri(request);

        String query = redirect.getQuery();
        // no listenerName query param added
        if (query != null) {
            assertFalse(query.contains("listenerName"), "query=" + query);
        }
    }

    @Test
    public void testToRedirectUriPreservesExistingListenerNameForAdminPaths() {
        // If the original admin request already carries listenerName in the query string, leave it
        // there rather than dropping it.
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .httpUrl(WEB_URL)
                .brokerServiceListenerName("external")
                .build();

        URI request = URI.create("http://original-host:1234/admin?listenerName=fromClient");

        URI redirect = result.toRedirectUri(request);

        String query = redirect.getQuery();
        assertNotNull(query);
        assertTrue(query.contains("listenerName=fromClient"), "query=" + query);
    }

    @Test
    public void testToStringIncludesAllFields() {
        LookupResult result = LookupResult.builder()
                .type(LookupResult.Type.RedirectUrl)
                .brokerId(BROKER_ID)
                .httpUrl(WEB_URL)
                .authoritativeRedirect(true)
                .brokerServiceListenerName("broker-listener")
                .webServiceListenerName("web-listener")
                .build();

        String s = result.toString();
        assertTrue(s.contains("type=RedirectUrl"));
        assertTrue(s.contains("authoritativeRedirect=true"));
        assertTrue(s.contains("brokerServiceListenerName='broker-listener'"));
        assertTrue(s.contains("webServiceListenerName='web-listener'"));
    }
}
