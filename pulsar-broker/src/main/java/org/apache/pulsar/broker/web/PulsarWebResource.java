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
package org.apache.pulsar.broker.web;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.admin.AdminResource.POLICIES_READONLY_FLAG_PATH;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.RESOURCEGROUPS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.DynamicConfigurationResources;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.NamespaceResources.IsolationPolicyResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ResourceGroupResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.path.PolicyPath;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Web resources in Pulsar. It provides basic authorization functions.
 */
public abstract class PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(PulsarWebResource.class);

    static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private PulsarService pulsar;

    protected PulsarService pulsar() {
        if (pulsar == null) {
            pulsar = (PulsarService) servletContext.getAttribute(WebService.ATTRIBUTE_PULSAR_NAME);
        }

        return pulsar;
    }

    protected ServiceConfiguration config() {
        return pulsar().getConfiguration();
    }

    public static String path(String... parts) {
        return PolicyPath.path(parts);
    }

    public static String joinPath(String... parts) {
        return PolicyPath.joinPath(parts);
    }

    public static String splitPath(String source, int slice) {
        return PolicyPath.splitPath(source, slice);
    }

    /**
     * Gets a caller id (IP + role).
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        return (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    }

    public String originalPrincipal() {
        return httpRequest.getHeader(ORIGINAL_PRINCIPAL_HEADER);
    }

    public AuthenticationDataHttps clientAuthData() {
        return (AuthenticationDataHttps) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    }

    public boolean isRequestHttps() {
        return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    public static boolean isClientAuthenticated(String appId) {
        return appId != null;
    }

    private static void validateOriginalPrincipal(Set<String> proxyRoles, String authenticatedPrincipal,
                                                  String originalPrincipal) {
        if (proxyRoles.contains(authenticatedPrincipal)) {
            // Request has come from a proxy
            if (StringUtils.isBlank(originalPrincipal)) {
                log.warn("Original principal empty in request authenticated as {}", authenticatedPrincipal);
                throw new RestException(Status.UNAUTHORIZED,
                        "Original principal cannot be empty if the request is via proxy.");
            }
            if (proxyRoles.contains(originalPrincipal)) {
                log.warn("Original principal {} cannot be a proxy role ({})", originalPrincipal, proxyRoles);
                throw new RestException(Status.UNAUTHORIZED, "Original principal cannot be a proxy role");
            }
        }
    }

    protected boolean hasSuperUserAccess() {
        try {
            validateSuperUserAccess();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws WebApplicationException
     *             if not authorized
     */
    public void validateSuperUserAccess() {
        if (config().isAuthenticationEnabled()) {
            String appId = clientAppId();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Check super user access: Authenticated: {} -- Role: {}", uri.getRequestUri(),
                        isClientAuthenticated(appId), appId);
            }
            String originalPrincipal = originalPrincipal();
            validateOriginalPrincipal(pulsar.getConfiguration().getProxyRoles(), appId, originalPrincipal);

            if (pulsar.getConfiguration().getProxyRoles().contains(appId)) {

                CompletableFuture<Boolean> proxyAuthorizedFuture;
                CompletableFuture<Boolean> originalPrincipalAuthorizedFuture;

                try {
                    proxyAuthorizedFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(appId, clientAuthData());

                    originalPrincipalAuthorizedFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(originalPrincipal, clientAuthData());

                    if (!proxyAuthorizedFuture.get() || !originalPrincipalAuthorizedFuture.get()) {
                        throw new RestException(Status.UNAUTHORIZED,
                                String.format("Proxy not authorized for super-user operation (proxy:%s,original:%s)",
                                              appId, originalPrincipal));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error validating super-user access : " + e.getMessage(), e);
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
                log.debug("Successfully authorized {} (proxied by {}) as super-user",
                          originalPrincipal, appId);
            } else {
                if (config().isAuthorizationEnabled() && !pulsar.getBrokerService()
                        .getAuthorizationService()
                        .isSuperUser(appId, clientAuthData())
                        .join()) {
                    throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
                }
                log.debug("Successfully authorized {} as super-user",
                          appId);
            }
        }
    }

    /**
     * Checks that the http client role has admin access to the specified tenant.
     *
     * @param tenant
     *            the tenant id
     * @throws WebApplicationException
     *             if not authorized
     */
    protected void validateAdminAccessForTenant(String tenant) {
        try {
            validateAdminAccessForTenant(pulsar(), clientAppId(), originalPrincipal(), tenant, clientAuthData());
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant {}", tenant);
            throw new RestException(e);
        }
    }

    protected static void validateAdminAccessForTenant(PulsarService pulsar, String clientAppId,
                                                       String originalPrincipal, String tenant,
                                                       AuthenticationDataSource authenticationData)
            throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("check admin access on tenant: {} - Authenticated: {} -- role: {}", tenant,
                    (isClientAuthenticated(clientAppId)), clientAppId);
        }

        TenantInfo tenantInfo = pulsar.getPulsarResources().getTenantResources().get(path(POLICIES, tenant))
                .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Tenant does not exist"));

        if (pulsar.getConfiguration().isAuthenticationEnabled() && pulsar.getConfiguration().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId)) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            validateOriginalPrincipal(pulsar.getConfiguration().getProxyRoles(), clientAppId, originalPrincipal);

            if (pulsar.getConfiguration().getProxyRoles().contains(clientAppId)) {
                CompletableFuture<Boolean> isProxySuperUserFuture;
                CompletableFuture<Boolean> isOriginalPrincipalSuperUserFuture;
                try {
                    AuthorizationService authorizationService = pulsar.getBrokerService().getAuthorizationService();
                    isProxySuperUserFuture = authorizationService.isSuperUser(clientAppId, authenticationData);

                    isOriginalPrincipalSuperUserFuture =
                            authorizationService.isSuperUser(originalPrincipal, authenticationData);

                    boolean proxyAuthorized = isProxySuperUserFuture.get()
                            || authorizationService.isTenantAdmin(tenant, clientAppId,
                            tenantInfo, authenticationData).get();
                    boolean originalPrincipalAuthorized =
                            isOriginalPrincipalSuperUserFuture.get() || authorizationService.isTenantAdmin(tenant,
                                    originalPrincipal, tenantInfo, authenticationData).get();
                    if (!proxyAuthorized || !originalPrincipalAuthorized) {
                        throw new RestException(Status.UNAUTHORIZED,
                                String.format("Proxy not authorized to access resource (proxy:%s,original:%s)",
                                        clientAppId, originalPrincipal));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
                log.debug("Successfully authorized {} (proxied by {}) on tenant {}",
                          originalPrincipal, clientAppId, tenant);
            } else {
                if (!pulsar.getBrokerService()
                        .getAuthorizationService()
                        .isSuperUser(clientAppId, authenticationData)
                        .join()) {
                    if (!pulsar.getBrokerService().getAuthorizationService()
                            .isTenantAdmin(tenant, clientAppId, tenantInfo, authenticationData).get()) {
                        throw new RestException(Status.UNAUTHORIZED,
                                "Don't have permission to administrate resources on this tenant");
                    }
                }

                log.debug("Successfully authorized {} on tenant {}", clientAppId, tenant);
            }
        }
    }

    protected void validateClusterForTenant(String tenant, String cluster) {
        TenantInfo tenantInfo;
        try {
            tenantInfo = pulsar().getPulsarResources().getTenantResources().get(path(POLICIES, tenant))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Tenant does not exist"));
        } catch (RestException e) {
            log.warn("Failed to get tenant admin data for tenant {}", tenant);
            throw e;
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant {}", tenant, e);
            throw new RestException(e);
        }

        // Check if tenant is allowed on the cluster
        if (!tenantInfo.getAllowedClusters().contains(cluster)) {
            String msg = String.format("Cluster [%s] is not in the list of allowed clusters list for tenant [%s]",
                    cluster, tenant);
            log.info(msg);
            throw new RestException(Status.FORBIDDEN, msg);
        }
        log.info("Successfully validated clusters on tenant [{}]", tenant);
    }

    /**
     * Check if the cluster exists and redirect the call to the owning cluster.
     *
     * @param cluster Cluster name
     * @throws Exception In case the redirect happens
     */
    protected void validateClusterOwnership(String cluster) throws WebApplicationException {
        try {
            ClusterData differentClusterData = getClusterDataIfDifferentCluster(pulsar(), cluster, clientAppId()).get();
            if (differentClusterData != null) {
                URI redirect = getRedirectionUrl(differentClusterData);
                // redirect to the cluster requested
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, cluster);
                }
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            if (e.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) e.getCause();
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String
                    .format("Failed to validate Cluster configuration : cluster=%s  emsg=%s", cluster, e.getMessage()));
        }

    }

    private URI getRedirectionUrl(ClusterData differentClusterData) throws MalformedURLException {
        try {
            PulsarServiceNameResolver serviceNameResolver = new PulsarServiceNameResolver();
            if (isRequestHttps() && pulsar.getConfiguration().getWebServicePortTls().isPresent()
                    && StringUtils.isNotBlank(differentClusterData.getServiceUrlTls())) {
                serviceNameResolver.updateServiceUrl(differentClusterData.getServiceUrlTls());
            } else {
                serviceNameResolver.updateServiceUrl(differentClusterData.getServiceUrl());
            }
            URL webUrl = new URL(serviceNameResolver.resolveHostUri().toString());
            return UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort()).build();
        } catch (PulsarClientException.InvalidServiceURL exception) {
            throw new MalformedURLException(exception.getMessage());
        }
    }

    protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
         String cluster, String clientAppId) {

        CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();

        if (!isValidCluster(pulsar, cluster)) {
            try {
                // this code should only happen with a v1 namespace format prop/cluster/namespaces
                if (!pulsar.getConfiguration().getClusterName().equals(cluster)) {
                    // redirect to the cluster requested
                    pulsar.getConfigurationCache().clustersCache().getAsync(path("clusters", cluster))
                            .thenAccept(clusterDataResult -> {
                                if (clusterDataResult.isPresent()) {
                                    clusterDataFuture.complete(clusterDataResult.get());
                                } else {
                                    log.warn("[{}] Cluster does not exist: requested={}", clientAppId, cluster);
                                    clusterDataFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                                            "Cluster does not exist: cluster=" + cluster));
                                }
                            }).exceptionally(ex -> {
                                clusterDataFuture.completeExceptionally(ex);
                                return null;
                            });
                } else {
                    clusterDataFuture.complete(null);
                }
            } catch (Exception e) {
                clusterDataFuture.completeExceptionally(e);
            }
        } else {
            clusterDataFuture.complete(null);
        }
        return clusterDataFuture;
    }

    static boolean isValidCluster(PulsarService pulsarService, String cluster) {// If the cluster name is
        // cluster == null or "global", don't validate the
        // cluster ownership. Cluster will be null in v2 naming.
        // The validation will be done by checking the namespace configuration
        if (cluster == null || Constants.GLOBAL_CLUSTER.equals(cluster)) {
            return true;
        }

        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // Without authorization, any cluster name should be valid and accepted by the broker
            return true;
        }
        return false;
    }

    /**
     * Checks whether the broker is the owner of all the namespace bundles. Otherwise, if authoritative is false, it
     * will throw an exception to redirect to assigned owner or leader; if authoritative is true then it will try to
     * acquire all the namespace bundles.
     *
     * @param tenant tenant name
     * @param cluster cluster name
     * @param namespace namespace name
     * @param authoritative if it is an authoritative request
     * @param readOnly if the request is read-only
     * @param bundleData bundle data
     */
    protected void validateNamespaceOwnershipWithBundles(String tenant, String cluster, String namespace,
            boolean authoritative, boolean readOnly, BundlesData bundleData) {
        NamespaceName fqnn = NamespaceName.get(tenant, cluster, namespace);

        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(fqnn,
                    bundleData);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                validateBundleOwnership(bundle, authoritative, readOnly);
            }
        } catch (WebApplicationException wae) {
            // propagate already wrapped-up WebApplicationExceptions
            throw wae;
        } catch (Exception oe) {
            log.debug("Failed to find owner for namespace {}", fqnn, oe);
            throw new RestException(oe);
        }
    }

    protected void validateBundleOwnership(String tenant, String cluster, String namespace, boolean authoritative,
            boolean readOnly, NamespaceBundle bundle) {
        NamespaceName fqnn = NamespaceName.get(tenant, cluster, namespace);

        try {
            validateBundleOwnership(bundle, authoritative, readOnly);
        } catch (WebApplicationException wae) {
            // propagate already wrapped-up WebApplicationExceptions
            throw wae;
        } catch (Exception oe) {
            log.debug("Failed to find owner for namespace {}", fqnn, oe);
            throw new RestException(oe);
        }
    }

    protected NamespaceBundle validateNamespaceBundleRange(NamespaceName fqnn, BundlesData bundles,
            String bundleRange) {
        try {
            checkArgument(bundleRange.contains("_"), "Invalid bundle range: " + bundleRange);
            String[] boundaries = bundleRange.split("_");
            Long lowerEndpoint = Long.decode(boundaries[0]);
            Long upperEndpoint = Long.decode(boundaries[1]);
            Range<Long> hashRange = Range.range(lowerEndpoint, BoundType.CLOSED, upperEndpoint,
                    (upperEndpoint.equals(NamespaceBundles.FULL_UPPER_BOUND)) ? BoundType.CLOSED : BoundType.OPEN);
            NamespaceBundle nsBundle = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundle(fqnn,
                    hashRange);
            NamespaceBundles nsBundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(fqnn,
                    bundles);
            nsBundles.validateBundle(nsBundle);
            return nsBundle;
        } catch (IllegalArgumentException e) {
            log.error("[{}] Invalid bundle range {}/{}, {}", clientAppId(), fqnn.toString(),
                    bundleRange, e.getMessage());
            throw new RestException(Response.Status.PRECONDITION_FAILED, e.getMessage());
        } catch (Exception e) {
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(),
                    fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    /**
     * Checks whether a given bundle is currently loaded by any broker.
     */
    protected CompletableFuture<Boolean> isBundleOwnedByAnyBroker(NamespaceName fqnn, BundlesData bundles,
            String bundleRange) {
        NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, bundles, bundleRange);
        NamespaceService nsService = pulsar().getNamespaceService();

        LookupOptions options = LookupOptions.builder()
                .authoritative(false)
                .requestHttps(isRequestHttps())
                .readOnly(true)
                .loadTopicsInBundle(false).build();
        try {
            return nsService.getWebServiceUrlAsync(nsBundle, options).thenApply(optionUrl -> optionUrl.isPresent());
        } catch (Exception e) {
            log.error("Failed to check whether namespace bundle is owned {}/{}", fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    protected NamespaceBundle validateNamespaceBundleOwnership(NamespaceName fqnn, BundlesData bundles,
            String bundleRange, boolean authoritative, boolean readOnly) {
        try {
            NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, bundles, bundleRange);
            validateBundleOwnership(nsBundle, authoritative, readOnly);
            return nsBundle;
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(),
                    fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    public void validateBundleOwnership(NamespaceBundle bundle, boolean authoritative, boolean readOnly)
            throws Exception {
        NamespaceService nsService = pulsar().getNamespaceService();

        try {
            // Call getWebServiceUrl() to acquire or redirect the request
            // Get web service URL of owning broker.
            // 1: If namespace is assigned to this broker, continue
            // 2: If namespace is assigned to another broker, redirect to the webservice URL of another broker
            // authoritative flag is ignored
            // 3: If namespace is unassigned and readOnly is true, return 412
            // 4: If namespace is unassigned and readOnly is false:
            // - If authoritative is false and this broker is not leader, forward to leader
            // - If authoritative is false and this broker is leader, determine owner and forward w/ authoritative=true
            // - If authoritative is true, own the namespace and continue
            LookupOptions options = LookupOptions.builder()
                    .authoritative(authoritative)
                    .requestHttps(isRequestHttps())
                    .readOnly(readOnly)
                    .loadTopicsInBundle(false).build();
            Optional<URL> webUrl = nsService.getWebServiceUrl(bundle, options);
            // Ensure we get a url
            if (webUrl == null || !webUrl.isPresent()) {
                log.warn("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Failed to find ownership for ServiceUnit:" + bundle.toString());
            }

            if (!nsService.isServiceUnitOwned(bundle)) {
                boolean newAuthoritative = this.isLeaderBroker();
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.get().getHost())
                        .port(webUrl.get().getPort()).replaceQueryParam("authoritative", newAuthoritative).build();

                log.debug("{} is not a service unit owned", bundle);

                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (TimeoutException te) {
            String msg = String.format("Finding owner for ServiceUnit %s timed out", bundle);
            log.error(msg, te);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, msg);
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug("Failed to find owner for ServiceUnit {}", bundle, iae);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "ServiceUnit format is not expected. ServiceUnit " + bundle);
        } catch (IllegalStateException ise) {
            log.debug("Failed to find owner for ServiceUnit {}", bundle, ise);
            throw new RestException(Status.PRECONDITION_FAILED, "ServiceUnit bundle is actived. ServiceUnit " + bundle);
        } catch (NullPointerException e) {
            log.warn("Unable to get web service url");
            throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for ServiceUnit:" + bundle);
        } catch (WebApplicationException wae) {
            throw wae;
        }
    }

    /**
     * Checks whether the broker is the owner of the namespace. Otherwise it will raise an exception to redirect the
     * client to the appropriate broker. If no broker owns the namespace yet, this function will try to acquire the
     * ownership by default.
     *
     * @param topicName topic name
     * @param authoritative
     */
    protected void validateTopicOwnership(TopicName topicName, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        try {
            // per function name, this is trying to acquire the whole namespace ownership
            LookupOptions options = LookupOptions.builder()
                    .authoritative(authoritative)
                    .requestHttps(isRequestHttps())
                    .readOnly(false)
                    .loadTopicsInBundle(false).build();
            Optional<URL> webUrl = nsService.getWebServiceUrl(topicName, options);
            // Ensure we get a url
            if (webUrl == null || !webUrl.isPresent()) {
                log.info("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for topic:" + topicName);
            }

            if (!nsService.isServiceUnitOwned(topicName)) {
                boolean newAuthoritative = isLeaderBroker(pulsar());
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.get().getHost())
                        .port(webUrl.get().getPort()).replaceQueryParam("authoritative", newAuthoritative).build();
                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (TimeoutException te) {
            String msg = String.format("Finding owner for topic %s timed out", topicName);
            log.error(msg, te);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, msg);
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug("Failed to find owner for topic: {}", topicName, iae);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
        } catch (IllegalStateException ise) {
            log.debug("Failed to find owner for topic: {}", topicName, ise);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception oe) {
            log.debug("Failed to find owner for topic: {}", topicName, oe);
            throw new RestException(oe);
        }
    }

    /**
     * If the namespace is global, validate the following - 1. If replicated clusters are configured for this global
     * namespace 2. If local cluster belonging to this namespace is replicated 3. If replication is enabled for this
     * namespace <br/>
     * It validates if local cluster is part of replication-cluster. If local cluster is not part of the replication
     * cluster then it redirects request to peer-cluster if any of the peer-cluster is part of replication-cluster of
     * this namespace. If none of the cluster is part of the replication cluster then it fails the validation.
     *
     * @param namespace
     * @throws Exception
     */
    protected void validateGlobalNamespaceOwnership(NamespaceName namespace) {
        int timeout = pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds();
        try {
            ClusterData peerClusterData = checkLocalOrGetPeerReplicationCluster(pulsar(), namespace)
                    .get(timeout, SECONDS);
            // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request should be
            // redirect to the peer-cluster
            if (peerClusterData != null) {
                URI redirect = getRedirectionUrl(peerClusterData);
                // redirect to the cluster requested
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(),
                            redirect, peerClusterData);

                }
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while validating policy on {} ", timeout, namespace);
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) e.getCause();
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        }
    }

    public static CompletableFuture<ClusterData> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
            NamespaceName namespace) {
        if (!namespace.isGlobal()) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<ClusterData> validationFuture = new CompletableFuture<>();
        final String localCluster = pulsarService.getConfiguration().getClusterName();
        final String path = AdminResource.path(POLICIES, namespace.toString());

        pulsarService.getConfigurationCache().policiesCache().getAsync(path).thenAccept(policiesResult -> {
            if (policiesResult.isPresent()) {
                Policies policies = policiesResult.get();
                if (policies.replication_clusters.isEmpty()) {
                    String msg = String.format(
                            "Namespace does not have any clusters configured : local_cluster=%s ns=%s",
                            localCluster, namespace.toString());
                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                } else if (!policies.replication_clusters.contains(localCluster)) {
                    ClusterData ownerPeerCluster = getOwnerFromPeerClusterList(pulsarService,
                            policies.replication_clusters);
                    if (ownerPeerCluster != null) {
                        // found a peer that own this namespace
                        validationFuture.complete(ownerPeerCluster);
                        return;
                    }
                    String msg = String.format(
                            "Namespace missing local cluster name in clusters list: local_cluster=%s ns=%s clusters=%s",
                            localCluster, namespace.toString(), policies.replication_clusters);

                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                } else {
                    validationFuture.complete(null);
                }
            } else {
                String msg = String.format("Policies not found for %s namespace", namespace.toString());
                log.warn(msg);
                validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND, msg));
            }
        }).exceptionally(ex -> {
            String msg = String.format("Failed to validate global cluster configuration : cluster=%s ns=%s  emsg=%s",
                    localCluster, namespace, ex.getMessage());
            log.error(msg);
            validationFuture.completeExceptionally(new RestException(ex));
            return null;
        });
        return validationFuture;
    }

    private static ClusterData getOwnerFromPeerClusterList(PulsarService pulsar, Set<String> replicationClusters) {
        String currentCluster = pulsar.getConfiguration().getClusterName();
        if (replicationClusters == null || replicationClusters.isEmpty() || isBlank(currentCluster)) {
            return null;
        }

        try {
            Optional<ClusterData> cluster = pulsar.getConfigurationCache().clustersCache()
                    .get(path("clusters", currentCluster));
            if (!cluster.isPresent() || cluster.get().getPeerClusterNames() == null) {
                return null;
            }
            for (String peerCluster : cluster.get().getPeerClusterNames()) {
                if (replicationClusters.contains(peerCluster)) {
                    return pulsar.getConfigurationCache().clustersCache().get(path("clusters", peerCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Peer cluster " + peerCluster + " data not found"));
                }
            }
        } catch (Exception e) {
            log.error("Failed to get peer-cluster {}-{}", currentCluster, e.getMessage());
            if (e instanceof RestException) {
                throw (RestException) e;
            } else {
                throw new RestException(e);
            }
        }
        return null;
    }

    protected void checkConnect(TopicName topicName) throws Exception {
        checkAuthorization(pulsar(), topicName, clientAppId(), clientAuthData());
    }

    protected static void checkAuthorization(PulsarService pulsarService, TopicName topicName, String role,
            AuthenticationDataSource authenticationData) throws Exception {
        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // No enforcing of authorization policies
            return;
        }
        // get zk policy manager
        if (!pulsarService.getBrokerService().getAuthorizationService().canLookup(topicName, role,
                authenticationData)) {
            log.warn("[{}] Role {} is not allowed to lookup topic", topicName, role);
            throw new RestException(Status.UNAUTHORIZED, "Don't have permission to connect to this namespace");
        }
    }

    // Used for unit tests access
    public void setPulsar(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    protected boolean isLeaderBroker() {
        return isLeaderBroker(pulsar());
    }

    protected static boolean isLeaderBroker(PulsarService pulsar) {
        return  pulsar.getLeaderElectionService().isLeader();
    }

    // Non-Usual HTTP error codes
    protected static final int NOT_IMPLEMENTED = 501;

    public void validateTenantOperation(String tenant, TenantOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.UNAUTHORIZED, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                .allowTenantOperation(tenant, operation, originalPrincipal(), clientAppId(), clientAuthData());
            if (!isAuthorized) {
                throw new RestException(Status.UNAUTHORIZED,
                    String.format("Unauthorized to validateTenantOperation for"
                            + " originalPrincipal [%s] and clientAppId [%s] about operation [%s] on tenant [%s]",
                        originalPrincipal(), clientAppId(), operation.toString(), tenant));
            }
        }
    }

    public void validateNamespaceOperation(NamespaceName namespaceName, NamespaceOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespaceOperation(namespaceName, operation, originalPrincipal(),
                        clientAppId(), clientAuthData());

            if (!isAuthorized) {
                throw new RestException(Status.FORBIDDEN,
                        String.format("Unauthorized to validateNamespaceOperation for"
                                + " operation [%s] on namespace [%s]", operation.toString(), namespaceName));
            }
        }
    }

    public void validateNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespacePolicyOperation(namespaceName, policy, operation,
                        originalPrincipal(), clientAppId(), clientAuthData());

            if (!isAuthorized) {
                throw new RestException(Status.FORBIDDEN,
                        String.format("Unauthorized to validateNamespacePolicyOperation for"
                                        + " operation [%s] on namespace [%s] on policy [%s]",
                                operation.toString(), namespaceName, policy.toString()));
            }
        }
    }

    protected PulsarResources getPulsarResources() {
        return pulsar().getPulsarResources();
    }

    protected TenantResources tenantResources() {
        return pulsar().getPulsarResources().getTenantResources();
    }

    protected ClusterResources clusterResources() {
        return pulsar().getPulsarResources().getClusterResources();
    }

    protected NamespaceResources namespaceResources() {
        return pulsar().getPulsarResources().getNamespaceResources();
    }

    protected ResourceGroupResources resourceGroupResources() {
        return pulsar().getPulsarResources().getResourcegroupResources();
    }

    protected LocalPoliciesResources getLocalPolicies() {
        return pulsar().getPulsarResources().getLocalPolicies();
    }

    protected IsolationPolicyResources namespaceIsolationPolicies(){
        return namespaceResources().getIsolationPolicies();
    }

    protected DynamicConfigurationResources dynamicConfigurationResources() {
        return pulsar().getPulsarResources().getDynamicConfigResources();
    }

    public static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    public void validatePoliciesReadOnlyAccess() {
        try {
            if (clusterResources().existsAsync(AdminResource.POLICIES_READONLY_FLAG_PATH).get()) {
                log.debug("Policies are read-only. Broker cannot do read-write operations");
                throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
            }
        } catch (Exception e) {
            log.warn("Unable to fetch read-only policy config {}", POLICIES_READONLY_FLAG_PATH, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> hasActiveNamespace(String tenant) {
        CompletableFuture<Void> activeNamespaceFuture = new CompletableFuture<>();
        tenantResources().getChildrenAsync(path(POLICIES, tenant)).thenAccept(clusterOrNamespaceList -> {
            if (clusterOrNamespaceList == null || clusterOrNamespaceList.isEmpty()) {
                activeNamespaceFuture.complete(null);
                return;
            }
            List<CompletableFuture<Void>> activeNamespaceListFuture = Lists.newArrayList();
            clusterOrNamespaceList.forEach(clusterOrNamespace -> {
                // get list of active V1 namespace
                CompletableFuture<Void> checkNs = new CompletableFuture<>();
                activeNamespaceListFuture.add(checkNs);
                tenantResources().getChildrenAsync(path(POLICIES, tenant, clusterOrNamespace))
                        .whenComplete((children, ex) -> {
                            if (ex != null) {
                                checkNs.completeExceptionally(ex);
                                return;
                            }
                            if (children != null && !children.isEmpty()) {
                                checkNs.completeExceptionally(
                                        new RestException(Status.PRECONDITION_FAILED, "Tenant has active namespace"));
                                return;
                            }
                            String namespace = NamespaceName.get(tenant, clusterOrNamespace).toString();
                            // if the length is 0 then this is probably a leftover cluster from namespace
                            // created
                            // with the v1 admin format (prop/cluster/ns) and then deleted, so no need to
                            // add it to the list
                            namespaceResources().getAsync(path(POLICIES, namespace)).thenApply(data -> {
                                if (data.isPresent()) {
                                    checkNs.completeExceptionally(new RestException(Status.PRECONDITION_FAILED,
                                            "Tenant has active namespace"));
                                } else {
                                    checkNs.complete(null);
                                }
                                return null;
                            }).exceptionally(ex2 -> {
                                if (ex2.getCause() instanceof MetadataStoreException.ContentDeserializationException) {
                                    // it's not a valid namespace-node
                                    checkNs.complete(null);
                                } else {
                                    checkNs.completeExceptionally(
                                            new RestException(Status.INTERNAL_SERVER_ERROR, ex2.getCause()));
                                }
                                return null;
                            });
                        });
                FutureUtil.waitForAll(activeNamespaceListFuture).thenAccept(r -> {
                    activeNamespaceFuture.complete(null);
                }).exceptionally(ex -> {
                    activeNamespaceFuture.completeExceptionally(ex.getCause());
                    return null;
                });
            });
        }).exceptionally(ex -> {
            activeNamespaceFuture.completeExceptionally(ex.getCause());
            return null;
        });
        return activeNamespaceFuture;
    }

    protected void validateClusterExists(String cluster) {
        try {
            if (!clusterResources().get(path("clusters", cluster)).isPresent()) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> canUpdateCluster(String tenant, Set<String> oldClusters,
            Set<String> newClusters) {
        List<CompletableFuture<Void>> activeNamespaceFuture = Lists.newArrayList();
        for (String cluster : oldClusters) {
            if (Constants.GLOBAL_CLUSTER.equals(cluster) || newClusters.contains(cluster)) {
                continue;
            }
            CompletableFuture<Void> checkNs = new CompletableFuture<>();
            activeNamespaceFuture.add(checkNs);
            tenantResources().getChildrenAsync(path(POLICIES, tenant, cluster)).whenComplete((activeNamespaces, ex) -> {
                if (ex != null) {
                    log.warn("Failed to get namespaces under {}-{}, {}", tenant, cluster, ex.getCause().getMessage());
                    checkNs.completeExceptionally(ex.getCause());
                    return;
                }
                if (activeNamespaces.size() > 0) {
                    log.warn("{}/{} Active-namespaces {}", tenant, cluster, activeNamespaces);
                    checkNs.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, "Active namespaces"));
                    return;
                }
                checkNs.complete(null);
            });
        }
        return activeNamespaceFuture.isEmpty() ? CompletableFuture.completedFuture(null)
                : FutureUtil.waitForAll(activeNamespaceFuture);
    }

    /**
     * Redirect the call to the specified broker.
     *
     * @param broker
     *            Broker name
     */
    protected void validateBrokerName(String broker) {
        String brokerUrl = String.format("http://%s", broker);
        String brokerUrlTls = String.format("https://%s", broker);
        if (!brokerUrl.equals(pulsar().getSafeWebServiceAddress())
                && !brokerUrlTls.equals(pulsar().getWebServiceAddressTls())) {
            String[] parts = broker.split(":");
            checkArgument(parts.length == 2, String.format("Invalid broker url %s", broker));
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(host).port(port).build();
            log.debug("[{}] Redirecting the rest call to {}: broker={}", clientAppId(), redirect, broker);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    /*
     * Get the list of namespaces (on every cluster) for a given property.
     *
     * @param property the property name
     * @return the list of namespaces
     */
    protected List<String> getListOfNamespaces(String tenant) throws Exception {
        List<String> namespaces = Lists.newArrayList();

        if (!tenantResources().exists(path(POLICIES, tenant))) {
            throw new RestException(Status.NOT_FOUND, tenant + " doesn't exist");
        }
        // this will return a cluster in v1 and a namespace in v2
        for (String clusterOrNamespace : tenantResources().getChildren(path(POLICIES, tenant))) {
            // Then get the list of namespaces
            final List<String> children = tenantResources().getChildren(path(POLICIES, tenant, clusterOrNamespace));
            if (children == null || children.isEmpty()) {
                String namespace = NamespaceName.get(tenant, clusterOrNamespace).toString();
                // if the length is 0 then this is probably a leftover cluster from namespace created
                // with the v1 admin format (prop/cluster/ns) and then deleted, so no need to add it to the list
                try {
                    if (namespaceResources().get(path(POLICIES, namespace)).isPresent()) {
                        namespaces.add(namespace);
                    }
                } catch (MetadataStoreException.ContentDeserializationException e) {
                    // not a namespace node
                }

            } else {
                children.forEach(ns -> {
                    namespaces.add(NamespaceName.get(tenant, clusterOrNamespace, ns).toString());
                });
            }
        }

        namespaces.sort(null);
        return namespaces;
    }

    /**
     * Get the list of resourcegroups.
     *
     * @return the list of resourcegroups
     */

    protected List<String> getListOfResourcegroups(String property) throws Exception {
        List<String> resourcegroups = Lists.newArrayList();

        for (String resourcegroup : resourceGroupResources().getChildren(path(RESOURCEGROUPS))) {
            resourcegroups.add(resourcegroup);
        }

        resourcegroups.sort(null);
        return resourcegroups;
    }


    public static void deleteRecursive(BaseResources resources, final String pathRoot) throws MetadataStoreException {
        PathUtils.validatePath(pathRoot);
        List<String> tree = listSubTreeBFS(resources, pathRoot);
        log.debug("Deleting {} with size {}", tree, tree.size());
        log.debug("Deleting " + tree.size() + " subnodes ");
        for (int i = tree.size() - 1; i >= 0; --i) {
            // Delete the leaves first and eventually get rid of the root
            resources.delete(tree.get(i));
        }
    }

    public static List<String> listSubTreeBFS(BaseResources resources, final String pathRoot)
            throws MetadataStoreException {
        Deque<String> queue = new LinkedList<>();
        List<String> tree = new ArrayList<>();
        queue.add(pathRoot);
        tree.add(pathRoot);
        while (true) {
            String node = queue.pollFirst();
            if (node == null) {
                break;
            }
            List<String> children = resources.getChildren(node);
            for (final String child : children) {
                final String childPath = node + "/" + child;
                queue.add(childPath);
                tree.add(childPath);
            }
        }
        return tree;
    }
}
