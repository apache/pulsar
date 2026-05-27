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

import static org.apache.pulsar.broker.lookup.v2.TopicLookup.LISTENERNAME_PARAM;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Represent a lookup result.
 *
 * Result can either be the lookup data describing the broker that owns the broker or the HTTP endpoint to where we need
 * to redirect the client to try again.
 */
public class LookupResult {
    public enum Type {
        BrokerUrl, RedirectUrl, LoadManagerMigrationUrl
    }

    private final Type type;
    private final boolean authoritativeRedirect;
    @Getter
    private final String brokerServiceListenerName;
    @Getter
    private final String webServiceListenerName;
    private final LookupData lookupData;

    @Builder
    private LookupResult(String brokerId, String httpUrl, String httpUrlTls, String brokerServiceUrl,
                         String brokerServiceUrlTls, Type type, boolean authoritativeRedirect,
                         String brokerServiceListenerName, String webServiceListenerName) {
        this.type = Objects.requireNonNull(type);
        this.authoritativeRedirect = authoritativeRedirect;
        this.brokerServiceListenerName = brokerServiceListenerName;
        this.webServiceListenerName = webServiceListenerName;
        this.lookupData = new LookupData(brokerId, brokerServiceUrl, brokerServiceUrlTls, httpUrl, httpUrlTls);
    }

    public static LookupResult create(BrokerLookupData selectedBroker, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(selectedBroker, options, Type.RedirectUrl, authoritativeRedirect);
    }

    public static LookupResult createLoadManagerMigrationLookupResult(BrokerLookupData selectedBroker,
                                                                      LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.LoadManagerMigrationUrl, false);
    }

    public static LookupResult create(BrokerLookupData selectedBroker, LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.BrokerUrl, false);
    }

    private static LookupResult internalCreate(BrokerLookupData selectedBroker, LookupOptions options,
                                               Type type, boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, selectedBroker.getBrokerId(),
                selectedBroker.getWebServiceUrl(), selectedBroker.getWebServiceUrlTls(),
                selectedBroker.getPulsarServiceUrl(), selectedBroker.getPulsarServiceUrlTls(),
                selectedBroker.advertisedListeners());
    }

    public static LookupResult create(LocalBrokerData selectedBroker, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(selectedBroker, options, Type.RedirectUrl, authoritativeRedirect);
    }

    public static LookupResult create(LocalBrokerData selectedBroker, LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.BrokerUrl, false);
    }

    private static LookupResult internalCreate(LocalBrokerData selectedBroker, LookupOptions options,
                                               Type type, boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, selectedBroker.getBrokerId(),
                selectedBroker.getWebServiceUrl(), selectedBroker.getWebServiceUrlTls(),
                selectedBroker.getPulsarServiceUrl(), selectedBroker.getPulsarServiceUrlTls(),
                selectedBroker.getAdvertisedListeners());
    }

    public static LookupResult create(NamespaceEphemeralData nsData, LookupOptions options) {
        return internalCreate(nsData, options, Type.BrokerUrl, false);
    }

    public static LookupResult create(NamespaceEphemeralData nsData, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(nsData, options, Type.RedirectUrl, authoritativeRedirect);
    }

    private static LookupResult internalCreate(NamespaceEphemeralData nsData, LookupOptions options, Type type,
                                               boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, nsData.getBrokerId(), nsData.getHttpUrl(),
                nsData.getHttpUrlTls(), nsData.getNativeUrl(), nsData.getNativeUrlTls(),
                nsData.getAdvertisedListeners());
    }

    private static LookupResult internalCreate(LookupOptions options, Type type,
                                               boolean authoritativeRedirect, String brokerId, String webServiceUrl,
                                               String webServiceUrlTls, String pulsarServiceUrl,
                                               String pulsarServiceUrlTls,
                                               Map<String, AdvertisedListener> advertisedListeners) {
        UrlOverride urls = new UrlOverride(webServiceUrl, webServiceUrlTls, pulsarServiceUrl, pulsarServiceUrlTls);

        AdvertisedListener brokerListener = lookupListener(options, advertisedListeners,
                LookupOptions::hasAdvertisedListenerName, LookupOptions::getAdvertisedListenerName);
        if (brokerListener != null) {
            urls.brokerServiceListenerName = options.getAdvertisedListenerName();
            urls.applyBrokerServiceOverride(brokerListener);
        }

        AdvertisedListener webListener = lookupListener(options, advertisedListeners,
                LookupOptions::hasWebServiceAdvertisedListenerName,
                LookupOptions::getWebServiceAdvertisedListenerName);
        if (webListener != null) {
            urls.webServiceListenerName = options.getWebServiceAdvertisedListenerName();
            urls.applyWebServiceOverride(webListener);
            // default the brokerServiceListenerName to the webServiceAdvertisedListenerName if the
            // listener also configures broker service URLs and no separate advertisedListenerName was given
            if (urls.brokerServiceListenerName == null
                    && (webListener.getBrokerServiceUrl() != null || webListener.getBrokerServiceUrlTls() != null)) {
                urls.brokerServiceListenerName = options.getWebServiceAdvertisedListenerName();
                urls.applyBrokerServiceOverride(webListener);
            }
        }

        // for backwards compatibility, derive the brokerId from webServiceUrl or webServiceUrlTls by
        // parsing the URL and taking host:port. This is a transient state that may occur during a
        // rolling upgrade when older brokers in the cluster do not yet publish a brokerId in their
        // ephemeral data; once all brokers have been upgraded, the brokerId field is always populated.
        if (brokerId == null && (webServiceUrl != null || webServiceUrlTls != null)) {
            URI url = URI.create(webServiceUrl != null ? webServiceUrl : webServiceUrlTls);
            if (url.getHost() != null && url.getPort() != -1) {
                brokerId = url.getHost() + ":" + url.getPort();
            }
        }

        return builder()
                .type(type)
                .brokerId(brokerId)
                .httpUrl(urls.httpUrl)
                .httpUrlTls(urls.httpUrlTls)
                .brokerServiceUrl(urls.brokerServiceUrl)
                .brokerServiceUrlTls(urls.brokerServiceUrlTls)
                .authoritativeRedirect(authoritativeRedirect)
                .brokerServiceListenerName(urls.brokerServiceListenerName)
                .webServiceListenerName(urls.webServiceListenerName)
                .build();
    }

    private static AdvertisedListener lookupListener(LookupOptions options,
                                                     Map<String, AdvertisedListener> advertisedListeners,
                                                     Predicate<LookupOptions> hasName,
                                                     Function<LookupOptions, String> getName) {
        if (options == null || !hasName.test(options)) {
            return null;
        }
        return advertisedListeners.get(getName.apply(options));
    }

    /** Mutable URL-set used while resolving listener-specific overrides for a LookupResult. */
    private static final class UrlOverride {
        String httpUrl;
        String httpUrlTls;
        String brokerServiceUrl;
        String brokerServiceUrlTls;
        String brokerServiceListenerName;
        String webServiceListenerName;

        UrlOverride(String httpUrl, String httpUrlTls, String brokerServiceUrl, String brokerServiceUrlTls) {
            this.httpUrl = httpUrl;
            this.httpUrlTls = httpUrlTls;
            this.brokerServiceUrl = brokerServiceUrl;
            this.brokerServiceUrlTls = brokerServiceUrlTls;
        }

        void applyBrokerServiceOverride(AdvertisedListener listener) {
            brokerServiceUrl = listener.getBrokerServiceUrl() != null
                    ? listener.getBrokerServiceUrl().toString() : null;
            brokerServiceUrlTls = listener.getBrokerServiceUrlTls() != null
                    ? listener.getBrokerServiceUrlTls().toString() : null;
        }

        void applyWebServiceOverride(AdvertisedListener listener) {
            httpUrl = listener.getBrokerHttpUrl() != null ? listener.getBrokerHttpUrl().toString() : null;
            httpUrlTls = listener.getBrokerHttpsUrl() != null ? listener.getBrokerHttpsUrl().toString() : null;
        }
    }

    public boolean isBrokerUrl() {
        return type == Type.BrokerUrl;
    }

    public boolean isRedirect() {
        return type == Type.RedirectUrl || type == Type.LoadManagerMigrationUrl;
    }

    public boolean isLoadManagerMigration() {
        return type == Type.LoadManagerMigrationUrl;
    }

    public boolean isAuthoritativeRedirect() {
        return authoritativeRedirect;
    }

    public LookupData getLookupData() {
        return lookupData;
    }

    /**
     * Creates a redirect URI by replacing the host, port and scheme of the given request URI with the
     * web service URL of this lookup result. The original path and query parameters are preserved.
     * The {@code authoritative} query parameter is set from this lookup result's
     * {@link #isAuthoritativeRedirect()} value when the result is a redirect, and removed otherwise.
     *
     * @param requestUri the incoming request URI (its scheme decides whether the HTTP or HTTPS
     *                   broker URL is used as the redirect target)
     * @return the redirect URI
     */
    public URI toRedirectUri(URI requestUri) {
        return toRedirectUriInternal(requestUri, this.authoritativeRedirect, false);
    }

    /**
     * Same as {@link #toRedirectUri(URI)} but overrides the {@code authoritative} query parameter
     * with the supplied value (e.g. when the current broker is the leader and must mark the
     * redirect as authoritative regardless of what the lookup result carried).
     */
    public URI toRedirectUri(URI requestUri, boolean authoritativeRedirectOverride) {
        return toRedirectUriInternal(requestUri, authoritativeRedirectOverride, false);
    }

    /**
     * Same as {@link #toRedirectUri(URI)} but specialised for topic-lookup redirects: when this
     * {@code LookupResult} has a resolved {@code brokerServiceListenerName} it is always written to
     * the {@code listenerName} query parameter on the redirect URI. This handles the case where the
     * original lookup request carried the listener name in a header rather than as a query
     * parameter — the header does not survive an HTTP redirect, so the parameter form must be
     * propagated to the next broker. Other (non-lookup) redirect paths use {@link #toRedirectUri}
     * and leave the parameter alone.
     */
    public URI toLookupRedirectUri(URI requestUri) {
        return toRedirectUriInternal(requestUri, this.authoritativeRedirect, true);
    }

    private URI toRedirectUriInternal(URI requestUri, boolean authoritativeRedirect,
                                      boolean injectListenerNameQueryParam) {
        boolean requireHttps = "https".equalsIgnoreCase(requestUri.getScheme());
        String webServiceUrl = requireHttps ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
        if (webServiceUrl == null) {
            // Preserve the legacy 412 error semantics when the redirect target broker has no URL
            // configured for the requested scheme.
            String scheme = requireHttps ? "https" : "http";
            StringBuilder entity = new StringBuilder()
                    .append("No ").append(scheme).append(" URL configured for broker ")
                    .append(lookupData.getBrokerId());
            if (StringUtils.isNotBlank(webServiceListenerName)) {
                entity.append(" on web service listener `").append(webServiceListenerName).append("`");
            } else if (StringUtils.isNotBlank(brokerServiceListenerName)) {
                entity.append(" on listener `").append(brokerServiceListenerName).append("`");
            }
            throw new WebApplicationException(Response.status(Response.Status.PRECONDITION_FAILED)
                    .entity(entity.toString())
                    .build());
        }
        URI webServiceUri = URI.create(webServiceUrl);
        UriBuilder uriBuilder =
                UriBuilder.fromUri(requestUri) // use the path and query parameters from the request URI
                        .scheme(webServiceUri.getScheme()) // use the schema from the lookup result
                        .host(webServiceUri.getHost())  // use the host from the lookup result
                        .port(webServiceUri.getPort()); // use the port from the lookup result
        if (isRedirect()) {
            // pass the authoritative parameter only when the type is redirect
            uriBuilder.replaceQueryParam("authoritative", authoritativeRedirect);
        } else {
            // remove the parameter when the type is not redirect
            uriBuilder.replaceQueryParam("authoritative");
        }
        // Only set the listenerName query parameter on topic-lookup redirects. The original lookup
        // request can carry it either as a query parameter or as a header; the latter does not
        // survive an HTTP redirect, so the resolved listener name must be reinjected as a query
        // parameter so the next broker sees it. Other redirect paths (admin endpoints) do not
        // understand `listenerName` as a parameter, so for those we leave the request URI alone.
        if (injectListenerNameQueryParam && StringUtils.isNotBlank(brokerServiceListenerName)) {
            uriBuilder.replaceQueryParam(LISTENERNAME_PARAM, brokerServiceListenerName);
        }
        return uriBuilder.build();
    }

    @Override
    public String toString() {
        return "LookupResult{"
                + "type=" + type
                + ", lookupData=" + lookupData
                + ", authoritativeRedirect=" + authoritativeRedirect
                + ", brokerServiceListenerName='" + brokerServiceListenerName + '\''
                + ", webServiceListenerName='" + webServiceListenerName + '\''
                + '}';
    }
}
