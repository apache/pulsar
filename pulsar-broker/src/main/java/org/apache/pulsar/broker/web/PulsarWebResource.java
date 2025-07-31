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
package org.apache.pulsar.broker.web;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.BookieResources;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.DynamicConfigurationResources;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.NamespaceResources.IsolationPolicyResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ResourceGroupResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.path.PolicyPath;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Web resources in Pulsar. It provides basic authorization functions.
 */
public abstract class PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(PulsarWebResource.class);

    private static final LoadingCache<String, PulsarServiceNameResolver> SERVICE_NAME_RESOLVER_CACHE =
            Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(5)).build(
                    new CacheLoader<>() {
                        @Override
                        public @Nullable PulsarServiceNameResolver load(@NonNull String serviceUrl) throws Exception {
                            PulsarServiceNameResolver serviceNameResolver = new PulsarServiceNameResolver();
                            serviceNameResolver.updateServiceUrl(serviceUrl);
                            return serviceNameResolver;
                        }
                    });

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

    public static String splitPath(String source, int slice) {
        return PolicyPath.splitPath(source, slice);
    }

    public AuthenticationParameters authParams() {
        return AuthenticationParameters.builder()
                .originalPrincipal(originalPrincipal())
                .clientRole(clientAppId())
                .clientAuthenticationDataSource(clientAuthData())
                .build();
    }

    /**
     * Gets a caller id (IP + role).
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        return httpRequest != null
                ? (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName) : null;
    }

    public String originalPrincipal() {
        return httpRequest != null ? httpRequest.getHeader(ORIGINAL_PRINCIPAL_HEADER) : null;
    }

    public AuthenticationDataSource clientAuthData() {
        return (AuthenticationDataSource) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    }

    public boolean isRequestHttps() {
        return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    public static boolean isClientAuthenticated(String appId) {
        return appId != null;
    }

    private void validateOriginalPrincipal(String authenticatedPrincipal, String originalPrincipal) {
        if (!pulsar.getBrokerService().getAuthorizationService()
                .isValidOriginalPrincipal(authenticatedPrincipal, originalPrincipal, clientAuthData())) {
            throw new RestException(Status.UNAUTHORIZED,
                    "Invalid combination of Original principal cannot be empty if the request is via proxy.");
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

    public CompletableFuture<Void> validateSuperUserAccessAsync() {
        if (!config().isAuthenticationEnabled() || !config().isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        String appId = clientAppId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Check super user access: Authenticated: {} -- Role: {}", uri.getRequestUri(),
                    isClientAuthenticated(appId), appId);
        }
        String originalPrincipal = originalPrincipal();
        validateOriginalPrincipal(appId, originalPrincipal);

        if (pulsar.getConfiguration().getProxyRoles().contains(appId)) {
            BrokerService brokerService = pulsar.getBrokerService();
            return brokerService.getAuthorizationService().isSuperUser(appId, clientAuthData())
                    .thenCompose(proxyAuthorizationSuccess -> {
                        if (!proxyAuthorizationSuccess){
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Proxy not authorized for super-user "
                                            + "operation (proxy:%s)", appId));
                        }
                        return pulsar.getBrokerService()
                                .getAuthorizationService()
                                .isSuperUser(originalPrincipal, clientAuthData());
                    }).thenAccept(originalPrincipalAuthorizationSuccess -> {
                        if (!originalPrincipalAuthorizationSuccess){
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Original principal not authorized for super-user operation "
                                                    + "(original:%s)", originalPrincipal));
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Successfully authorized {} (proxied by {}) as super-user",
                                    originalPrincipal, appId);
                        }
                    });
        } else {
            return pulsar.getBrokerService()
                    .getAuthorizationService()
                    .isSuperUser(appId, clientAuthData())
                    .thenAccept(proxyAuthorizationSuccess -> {
                        if (!proxyAuthorizationSuccess) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    "This operation requires super-user access");
                        }
                    });
        }
    }

    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws WebApplicationException
     *             if not authorized
     */
    public void validateSuperUserAccess() {
        sync(this::validateSuperUserAccessAsync);
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
            validateAdminAccessForTenant(pulsar(), clientAppId(), originalPrincipal(), tenant, clientAuthData(),
                    config().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant {}", tenant);
            throw new RestException(e);
        }
    }

    protected void validateAdminAccessForTenant(PulsarService pulsar, String clientAppId,
                                                String originalPrincipal, String tenant,
                                                AuthenticationDataSource authenticationData,
                                                long timeout, TimeUnit unit) {
        try {
            validateAdminAccessForTenantAsync(pulsar, clientAppId, originalPrincipal, tenant, authenticationData)
                    .get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Throwable realCause = FutureUtil.unwrapCompletionException(e);
            if (realCause instanceof WebApplicationException) {
                throw (WebApplicationException) realCause;
            } else {
                throw new RestException(realCause);
            }
        }
    }

    /**
     * Checks that the http client role has admin access to the specified tenant async.
     *
     * @param tenant the tenant id
     */
    protected CompletableFuture<Void> validateAdminAccessForTenantAsync(String tenant) {
        return validateAdminAccessForTenantAsync(pulsar(), clientAppId(), originalPrincipal(), tenant,
                clientAuthData());
    }

    protected CompletableFuture<Void> validateAdminAccessForTenantAsync(
            PulsarService pulsar, String clientAppId,
            String originalPrincipal, String tenant,
            AuthenticationDataSource authenticationData) {
        if (log.isDebugEnabled()) {
            log.debug("check admin access on tenant: {} - Authenticated: {} -- role: {}", tenant,
                    (isClientAuthenticated(clientAppId)), clientAppId);
        }
        return pulsar.getPulsarResources().getTenantResources().getTenantAsync(tenant)
                .thenCompose(tenantInfoOptional -> {
                    if (!tenantInfoOptional.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
                    }
                    TenantInfo tenantInfo = tenantInfoOptional.get();
                    if (pulsar.getConfiguration().isAuthenticationEnabled() && pulsar.getConfiguration()
                            .isAuthorizationEnabled()) {
                        if (!isClientAuthenticated(clientAppId)) {
                            throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
                        }
                        validateOriginalPrincipal(clientAppId, originalPrincipal);
                        if (pulsar.getConfiguration().getProxyRoles().contains(clientAppId)) {
                            AuthorizationService authorizationService =
                                    pulsar.getBrokerService().getAuthorizationService();
                            return authorizationService.isTenantAdmin(tenant, clientAppId, tenantInfo,
                                            authenticationData)
                                .thenCompose(isTenantAdmin -> {
                                    String debugMsg = "Successfully authorized {} (proxied by {}) on tenant {}";
                                    if (!isTenantAdmin) {
                                            return authorizationService.isSuperUser(clientAppId, authenticationData)
                                                .thenCombine(authorizationService.isSuperUser(originalPrincipal,
                                                             authenticationData),
                                                     (proxyAuthorized, originalPrincipalAuthorized) -> {
                                                         if (!proxyAuthorized || !originalPrincipalAuthorized) {
                                                             throw new RestException(Status.UNAUTHORIZED,
                                                                     String.format("Proxy not authorized to access "
                                                                                     + "resource (proxy:%s,original:%s)"
                                                                             , clientAppId, originalPrincipal));
                                                         } else {
                                                             if (log.isDebugEnabled()) {
                                                                 log.debug(debugMsg, originalPrincipal, clientAppId,
                                                                         tenant);
                                                             }
                                                             return null;
                                                         }
                                                     });
                                    } else {
                                        if (log.isDebugEnabled()) {
                                            log.debug(debugMsg, originalPrincipal, clientAppId, tenant);
                                        }
                                        return CompletableFuture.completedFuture(null);
                                    }
                                });
                        } else {
                            return pulsar.getBrokerService()
                                    .getAuthorizationService()
                                    .isSuperUser(clientAppId, authenticationData)
                                    .thenCompose(isSuperUser -> {
                                        if (!isSuperUser) {
                                            return pulsar.getBrokerService().getAuthorizationService()
                                                    .isTenantAdmin(tenant, clientAppId, tenantInfo, authenticationData);
                                        } else {
                                            return CompletableFuture.completedFuture(true);
                                        }
                                    }).thenAccept(authorized -> {
                                        if (!authorized) {
                                            throw new RestException(Status.UNAUTHORIZED,
                                                    "Don't have permission to administrate resources on this tenant");
                                        } else {
                                            log.debug("Successfully authorized {} on tenant {}", clientAppId, tenant);
                                        }
                                    });
                        }
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * It validates that peer-clusters can't coexist in replication-clusters.
     *
     * @clusterName: given cluster whose peer-clusters can't be present into replication-cluster list
     * @replicationClusters: replication-cluster list
     */
    protected void validatePeerClusterConflict(String clusterName, Set<String> replicationClusters) {
        try {
            ClusterData clusterData = clusterResources().getCluster(clusterName).orElseThrow(
                    () -> new RestException(Status.PRECONDITION_FAILED, "Invalid replication cluster " + clusterName));
            Set<String> peerClusters = clusterData.getPeerClusterNames();
            if (peerClusters != null && !peerClusters.isEmpty()) {
                Sets.SetView<String> conflictPeerClusters = Sets.intersection(peerClusters, replicationClusters);
                if (!conflictPeerClusters.isEmpty()) {
                    log.warn("[{}] {}'s peer cluster can't be part of replication clusters {}", clientAppId(),
                            clusterName, conflictPeerClusters);
                    throw new RestException(Status.CONFLICT,
                            String.format("%s's peer-clusters %s can't be part of replication-clusters %s", clusterName,
                                    conflictPeerClusters, replicationClusters));
                }
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.warn("[{}] Failed to get cluster-data for {}", clientAppId(), clusterName, e);
        }
    }

    protected CompletableFuture<Void> validatePeerClusterConflictAsync(String clusterName,
                                                                       Set<String> replicationClusters) {
        return clusterResources().getClusterAsync(clusterName)
                .thenAccept(data -> {
                    ClusterData clusterData = data.orElseThrow(() -> new RestException(
                            Status.PRECONDITION_FAILED, "Invalid replication cluster " + clusterName));
                    Set<String> peerClusters = clusterData.getPeerClusterNames();
                    if (peerClusters != null && !peerClusters.isEmpty()) {
                        Sets.SetView<String> conflictPeerClusters =
                                Sets.intersection(peerClusters, replicationClusters);
                        if (!conflictPeerClusters.isEmpty()) {
                            log.warn("[{}] {}'s peer cluster can't be part of replication clusters {}", clientAppId(),
                                    clusterName, conflictPeerClusters);
                            throw new RestException(Status.CONFLICT,
                                    String.format("%s's peer-clusters %s can't be part of replication-clusters %s",
                                            clusterName,
                                            conflictPeerClusters, replicationClusters));
                        }
                    }
                });
    }

    protected void validateClusterForTenant(String tenant, String cluster) {
        TenantInfo tenantInfo;
        try {
            tenantInfo = pulsar().getPulsarResources().getTenantResources().getTenant(tenant)
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

    protected CompletableFuture<Void> validateClusterForTenantAsync(String tenant, String cluster) {
        return pulsar().getPulsarResources().getTenantResources().getTenantAsync(tenant)
                .thenAccept(tenantInfo -> {
                    if (!tenantInfo.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
                    }
                    if (!tenantInfo.get().getAllowedClusters().contains(cluster)) {
                        String msg = String.format("Cluster [%s] is not in the list of allowed clusters list"
                                        + " for tenant [%s]", cluster, tenant);
                        log.info(msg);
                        throw new RestException(Status.FORBIDDEN, msg);
                    }
                });
    }

    protected CompletableFuture<Void> validateClusterOwnershipAsync(String cluster) {
        return getClusterDataIfDifferentCluster(pulsar(), cluster, clientAppId())
                .thenAccept(differentClusterData -> {
                    if (differentClusterData != null) {
                        try {
                            URI redirect = getRedirectionUrl(differentClusterData);
                            // redirect to the cluster requested
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Redirecting the rest call to {}: cluster={}",
                                        clientAppId(), redirect, cluster);
                            }
                            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                        } catch (MalformedURLException ex) {
                            throw new RestException(ex);
                        }
                    }
                });
    }

    /**
     * Check if the cluster exists and redirect the call to the owning cluster.
     *
     * @param cluster Cluster name
     * @throws Exception In case the redirect happens
     */
    protected void validateClusterOwnership(String cluster) throws WebApplicationException {
        sync(()-> validateClusterOwnershipAsync(cluster));
    }

    private URI getRedirectionUrl(ClusterData differentClusterData) throws MalformedURLException {
        try {
            PulsarServiceNameResolver serviceNameResolver;
            if (isRequestHttps() && pulsar.getConfiguration().getWebServicePortTls().isPresent()
                    && StringUtils.isNotBlank(differentClusterData.getServiceUrlTls())) {
                serviceNameResolver = SERVICE_NAME_RESOLVER_CACHE.get(differentClusterData.getServiceUrlTls());
            } else {
                serviceNameResolver = SERVICE_NAME_RESOLVER_CACHE.get(differentClusterData.getServiceUrl());
            }
            URL webUrl = new URL(serviceNameResolver.resolveHostUri().toString());
            return UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort()).build();
        } catch (Exception exception) {
            if (exception.getCause() != null
                    && exception.getCause() instanceof PulsarClientException.InvalidServiceURL) {
                throw new MalformedURLException(exception.getMessage());
            }
            throw exception;
        }
    }

    protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
                                                                                     String cluster,
                                                                                     String clientAppId) {
        CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();
        if (isValidCluster(pulsar, cluster)
                // this code should only happen with a v1 namespace format prop/cluster/namespaces
                || pulsar.getConfiguration().getClusterName().equals(cluster)) {
            clusterDataFuture.complete(null);
            return clusterDataFuture;
        }
        // redirect to the cluster requested
        pulsar.getPulsarResources().getClusterResources().getClusterAsync(cluster)
                .whenComplete((clusterDataResult, ex) -> {
                    if (ex != null) {
                        log.warn("[{}] Load cluster data failed: requested={}", clientAppId, cluster);
                        clusterDataFuture.completeExceptionally(FutureUtil.unwrapCompletionException(ex));
                        return;
                    }
                    if (clusterDataResult.isPresent()) {
                        clusterDataFuture.complete(clusterDataResult.get());
                    } else {
                        log.warn("[{}] Cluster does not exist: requested={}", clientAppId, cluster);
                        clusterDataFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                                "Cluster does not exist: cluster=" + cluster));
                    }
                });
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

        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return nsService.checkOwnershipPresentAsync(nsBundle);
        }

        LookupOptions options = LookupOptions.builder()
                .authoritative(false)
                .requestHttps(isRequestHttps())
                .readOnly(true)
                .loadTopicsInBundle(false).build();

        return nsService.getWebServiceUrlAsync(nsBundle, options).thenApply(Optional::isPresent);
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

    protected CompletableFuture<NamespaceBundle> validateNamespaceBundleOwnershipAsync(
            NamespaceName fqnn, BundlesData bundles, String bundleRange,
            boolean authoritative, boolean readOnly) {
        NamespaceBundle nsBundle;
        try {
            nsBundle = validateNamespaceBundleRange(fqnn, bundles, bundleRange);
        } catch (WebApplicationException wae) {
            return CompletableFuture.failedFuture(wae);
        }
        return validateBundleOwnershipAsync(nsBundle, authoritative, readOnly)
                .thenApply(__ -> nsBundle);
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

    public CompletableFuture<Void> validateBundleOwnershipAsync(NamespaceBundle bundle, boolean authoritative,
                                                                boolean readOnly) {
        NamespaceService nsService = pulsar().getNamespaceService();
        LookupOptions options = LookupOptions.builder()
                .authoritative(authoritative)
                .requestHttps(isRequestHttps())
                .readOnly(readOnly)
                .loadTopicsInBundle(false).build();
        return nsService.getWebServiceUrlAsync(bundle, options)
                .thenCompose(webUrl -> {
                    if (webUrl == null || !webUrl.isPresent()) {
                        log.warn("Unable to get web service url");
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Failed to find ownership for ServiceUnit:" + bundle.toString());
                    }
                    return nsService.isServiceUnitOwnedAsync(bundle)
                            .thenAccept(owned -> {
                                if (!owned) {
                                    boolean newAuthoritative = this.isLeaderBroker();
                                    // Replace the host and port of the current request and redirect
                                    UriBuilder uriBuilder = UriBuilder.fromUri(uri.getRequestUri())
                                            .host(webUrl.get().getHost())
                                            .port(webUrl.get().getPort())
                                            .replaceQueryParam("authoritative", newAuthoritative);
                                    if (!ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
                                        uriBuilder.replaceQueryParam("destinationBroker", null);
                                    }
                                    URI redirect = uriBuilder.build();
                                    log.debug("{} is not a service unit owned", bundle);
                                    // Redirect
                                    log.debug("Redirecting the rest call to {}", redirect);
                                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                                }
                            });
                });
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
        sync(()-> validateTopicOwnershipAsync(topicName, authoritative));
    }

    protected CompletableFuture<Void> validateTopicOwnershipAsync(TopicName topicName, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        LookupOptions options = LookupOptions.builder()
                .authoritative(authoritative)
                .requestHttps(isRequestHttps())
                .readOnly(false)
                .loadTopicsInBundle(false)
                .build();

        return nsService.getWebServiceUrlAsync(topicName, options)
                .thenApply(webUrl -> {
                    // Ensure we get a url
                    if (webUrl == null || !webUrl.isPresent()) {
                        log.info("Unable to get web service url");
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Failed to find ownership for topic:" + topicName);
                    }
                    return webUrl.get();
                }).thenCompose(webUrl -> nsService.isServiceUnitOwnedAsync(topicName)
                        .thenApply(isTopicOwned -> Pair.of(webUrl, isTopicOwned))
                ).thenAccept(pair -> {
                    URL webUrl = pair.getLeft();
                    boolean isTopicOwned = pair.getRight();

                    if (!isTopicOwned) {
                        boolean newAuthoritative = isLeaderBroker(pulsar());
                        // Replace the host and port of the current request and redirect
                        URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                .host(webUrl.getHost())
                                .port(webUrl.getPort())
                                .replaceQueryParam("authoritative", newAuthoritative)
                                .build();
                        // Redirect
                        if (log.isDebugEnabled()) {
                            log.debug("Redirecting the rest call to {}", redirect);
                        }
                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                    }
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof IllegalArgumentException
                            || ex.getCause() instanceof IllegalStateException) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to find owner for topic: {}", topicName, ex);
                        }
                        throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
                    } else if (ex.getCause() instanceof WebApplicationException) {
                        throw (WebApplicationException) ex.getCause();
                    } else {
                        throw new RestException(ex.getCause());
                    }
                });
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
        int timeout = pulsar().getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        try {
            ClusterDataImpl peerClusterData = checkLocalOrGetPeerReplicationCluster(pulsar(), namespace)
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
            Throwable throwable = FutureUtil.unwrapCompletionException(e);
            if (throwable instanceof WebApplicationException webApplicationException) {
                throw webApplicationException;
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        }
    }

    protected CompletableFuture<Void> validateGlobalNamespaceOwnershipAsync(NamespaceName namespace) {
        return checkLocalOrGetPeerReplicationCluster(pulsar(), namespace)
                .thenAccept(peerClusterData -> {
                    // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request
                    // should be redirect to the peer-cluster
                    if (peerClusterData != null) {
                        try {
                            URI redirect = getRedirectionUrl(peerClusterData);
                            // redirect to the cluster requested
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(),
                                        redirect, peerClusterData);

                            }
                            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                        } catch (MalformedURLException mue) {
                            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace,
                                    mue.getMessage()));
                        }
                    }
                });
    }

    public static CompletableFuture<ClusterDataImpl> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
                                                                                           NamespaceName namespace) {
        return checkLocalOrGetPeerReplicationCluster(pulsarService, namespace, false);
    }
    public static CompletableFuture<ClusterDataImpl> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
                                                                                     NamespaceName namespace,
                                                                                     boolean allowDeletedNamespace) {
        if (!namespace.isGlobal() || NamespaceService.isHeartbeatNamespace(namespace)) {
            return CompletableFuture.completedFuture(null);
        }

        final CompletableFuture<ClusterDataImpl> validationFuture = new CompletableFuture<>();
        final String localCluster = pulsarService.getConfiguration().getClusterName();

        pulsarService.getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(namespace).thenAccept(policiesResult -> {
            if (policiesResult.isPresent()) {
                Policies policies = policiesResult.get();
                if (!allowDeletedNamespace && policies.deleted) {
                    String msg = String.format("Namespace %s is deleted", namespace.toString());
                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                            "Namespace is deleted"));
                } else if (policies.replication_clusters.isEmpty() && policies.allowed_clusters.isEmpty()) {
                    String msg = String.format(
                            "Namespace does not have any clusters configured : local_cluster=%s ns=%s",
                            localCluster, namespace.toString());
                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                } else if (!policies.replication_clusters.contains(localCluster) && !policies.allowed_clusters
                        .contains(localCluster)) {
                    getOwnerFromPeerClusterListAsync(pulsarService, policies.replication_clusters,
                            policies.allowed_clusters)
                            .thenAccept(ownerPeerCluster -> {
                                if (ownerPeerCluster != null) {
                                    // found a peer that own this namespace
                                    validationFuture.complete(ownerPeerCluster);
                                } else {
                                    String msg = String.format(
                                            "Namespace missing local cluster name in clusters list: local_cluster=%s"
                                                    + " ns=%s clusters=%s",
                                            localCluster, namespace.toString(), policies.replication_clusters);
                                    log.warn(msg);
                                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED,
                                            msg));
                                }
                            })
                            .exceptionally(ex -> {
                                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                                validationFuture.completeExceptionally(new RestException(cause));
                                return null;
                            });
                } else {
                    validationFuture.complete(null);
                }
            } else {
                String msg = String.format("Namespace %s not found", namespace.toString());
                log.warn(msg);
                validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND, "Namespace not found"));
            }
        }).exceptionally(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            String msg = String.format("Failed to validate global cluster configuration : cluster=%s ns=%s  emsg=%s",
                    localCluster, namespace, cause.getMessage());
            log.error(msg);
            validationFuture.completeExceptionally(new RestException(cause));
            return null;
        });
        return validationFuture;
    }

    private static CompletableFuture<ClusterDataImpl> getOwnerFromPeerClusterListAsync(PulsarService pulsar,
            Set<String> replicationClusters, Set<String> allowedClusters) {
        String currentCluster = pulsar.getConfiguration().getClusterName();
        if (replicationClusters.isEmpty() && allowedClusters.isEmpty() || isBlank(currentCluster)) {
            return CompletableFuture.completedFuture(null);
        }

        return pulsar.getPulsarResources().getClusterResources().getClusterAsync(currentCluster)
                .thenCompose(cluster -> {
                    if (!cluster.isPresent() || cluster.get().getPeerClusterNames() == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    for (String peerCluster : cluster.get().getPeerClusterNames()) {
                        if (replicationClusters.contains(peerCluster)
                                || allowedClusters.contains(peerCluster)) {
                            return pulsar.getPulsarResources().getClusterResources().getClusterAsync(peerCluster)
                                    .thenApply(ret -> {
                                        if (!ret.isPresent()) {
                                            throw new RestException(Status.NOT_FOUND,
                                                    "Peer cluster " + peerCluster + " data not found");
                                        }
                                        return (ClusterDataImpl) ret.get();
                                    });
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }).exceptionally(ex -> {
                    log.error("Failed to get peer-cluster {}-{}", currentCluster, ex.getMessage());
                    throw FutureUtil.wrapToCompletionException(ex);
                });
    }

    protected static CompletableFuture<Void> checkAuthorizationAsync(PulsarService pulsarService, TopicName topicName,
                        String role, AuthenticationDataSource authenticationData) {
        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // No enforcing of authorization policies
            return CompletableFuture.completedFuture(null);
        }
        // get zk policy manager
        return pulsarService.getBrokerService().getAuthorizationService().allowTopicOperationAsync(topicName,
                TopicOperation.LOOKUP, null, role, authenticationData).thenAccept(allow -> {
                    if (!allow) {
                        log.warn("[{}] Role {} is not allowed to lookup topic", topicName, role);
                        throw new RestException(Status.UNAUTHORIZED,
                                "Don't have permission to connect to this namespace");
                    }
        });
    }

    // Used for unit tests access
    public void setPulsar(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    protected boolean isLeaderBroker() {
        return isLeaderBroker(pulsar());
    }

    protected static boolean isLeaderBroker(PulsarService pulsar) {
        // For extensible load manager, it doesn't have leader election service on pulsar broker.
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return true;
        }
        return pulsar.getLeaderElectionService() != null && pulsar.getLeaderElectionService().isLeader();
    }

    public void validateTenantOperation(String tenant, TenantOperation operation) {
        sync(()-> validateTenantOperationAsync(tenant, operation));
    }

    public CompletableFuture<Void> validateTenantOperationAsync(String tenant, TenantOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(
                        new RestException(Status.UNAUTHORIZED, "Need to authenticate to perform the request"));
            }

            return pulsar().getBrokerService().getAuthorizationService()
                    .allowTenantOperationAsync(tenant, operation, originalPrincipal(), clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Unauthorized to validateTenantOperation for"
                                                    + " originalPrincipal [%s] and clientAppId [%s] "
                                                    + "about operation [%s] on tenant [%s]",
                                            originalPrincipal(), clientAppId(), operation.toString(), tenant));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public void validateNamespaceOperation(NamespaceName namespaceName, NamespaceOperation operation) {
        sync(()-> validateNamespaceOperationAsync(namespaceName, operation));
    }


    public CompletableFuture<Void> validateNamespaceOperationAsync(NamespaceName namespaceName,
                                                              NamespaceOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(
                        new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request"));
            }

            return pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespaceOperationAsync(namespaceName, operation, originalPrincipal(),
                             clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.FORBIDDEN,
                                    String.format("Unauthorized to validateNamespaceOperation for"
                                        + " operation [%s] on namespace [%s]", operation.toString(), namespaceName));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public void validateNamespacePolicyOperation(NamespaceName namespaceName, PolicyName policy,
                                                 PolicyOperation operation) {
        sync(()-> validateNamespacePolicyOperationAsync(namespaceName, policy, operation));
    }

    public CompletableFuture<Void> validateNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(
                        new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request"));
            }

            return pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespacePolicyOperationAsync(namespaceName, policy, operation,
                            originalPrincipal(), clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.FORBIDDEN,
                                    String.format("Unauthorized to validateNamespacePolicyOperation for"
                                                    + " operation [%s] on namespace [%s] on policy [%s]",
                                            operation.toString(), namespaceName, policy.toString()));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
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

    protected BookieResources bookieResources() {
        return pulsar().getPulsarResources().getBookieResources();
    }

    protected TopicResources topicResources() {
        return pulsar().getPulsarResources().getTopicResources();
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

    public void validatePoliciesReadOnlyAccess() {
        try {
            if (namespaceResources().getPoliciesReadOnly()) {
                log.debug("Policies are read-only. Broker cannot do read-write operations");
                throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
            }
        } catch (Exception e) {
            log.warn("Unable to fetch read-only policy config ", e);
            throw new RestException(e);
        }
    }

    public CompletableFuture<Void> validatePoliciesReadOnlyAccessAsync() {
        return namespaceResources().getPoliciesReadOnlyAsync().thenAccept(readOnly -> {
            if (readOnly) {
                if (log.isDebugEnabled()) {
                    log.debug("Policies are read-only. Broker cannot do read-write operations");
                }
                throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
            }
        });
    }

    protected CompletableFuture<Void> hasActiveNamespace(String tenant) {
        return tenantResources().hasActiveNamespace(tenant);
    }

    protected void validateClusterExists(String cluster) {
        try {
            if (!clusterResources().clusterExists(cluster)) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> canUpdateCluster(String tenant, Set<String> oldClusters,
            Set<String> newClusters) {
        List<CompletableFuture<Void>> activeNamespaceFuture = new ArrayList<>();
        for (String cluster : oldClusters) {
            if (Constants.GLOBAL_CLUSTER.equals(cluster) || newClusters.contains(cluster)) {
                continue;
            }
            CompletableFuture<Void> checkNs = new CompletableFuture<>();
            activeNamespaceFuture.add(checkNs);
            tenantResources().getActiveNamespaces(tenant, cluster).whenComplete((activeNamespaces, ex) -> {
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
        return FutureUtil.waitForAll(activeNamespaceFuture);
    }

    /**
     * Redirect the call to the specified broker.
     *
     * @param brokerId broker's id (lookup service address)
     */
    protected CompletableFuture<Void> maybeRedirectToBroker(String brokerId) {
        // backwards compatibility
        String cleanedBrokerId = brokerId.replaceFirst("http[s]?://", "");
        if (pulsar.getBrokerId().equals(cleanedBrokerId)
                // backwards compatibility
                || ("http://" + cleanedBrokerId).equals(pulsar().getWebServiceAddress())
                || ("https://" + cleanedBrokerId).equals(pulsar().getWebServiceAddressTls())) {
            // no need to redirect, the current broker matches the given broker id
            return CompletableFuture.completedFuture(null);
        }
        LockManager<BrokerLookupData> brokerLookupDataLockManager =
                pulsar().getCoordinationService().getLockManager(BrokerLookupData.class);
        return brokerLookupDataLockManager.readLock(LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + cleanedBrokerId)
                .thenAccept(brokerLookupDataOptional -> {
                    if (brokerLookupDataOptional.isEmpty()) {
                        throw new RestException(Status.NOT_FOUND,
                                "Broker id '" + brokerId + "' not found in available brokers.");
                    }
                    brokerLookupDataOptional.ifPresent(brokerLookupData -> {
                        URI targetBrokerUri;
                        if ((isRequestHttps() || StringUtils.isBlank(brokerLookupData.getWebServiceUrl()))
                                && StringUtils.isNotBlank(brokerLookupData.getWebServiceUrlTls())) {
                            targetBrokerUri = URI.create(brokerLookupData.getWebServiceUrlTls());
                        } else {
                            targetBrokerUri = URI.create(brokerLookupData.getWebServiceUrl());
                        }
                        URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                .scheme(targetBrokerUri.getScheme())
                                .host(targetBrokerUri.getHost())
                                .port(targetBrokerUri.getPort()).build();
                        log.debug("[{}] Redirecting the rest call to {}: broker={}", clientAppId(), redirect,
                                cleanedBrokerId);
                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                    });
                });
    }

    public void validateTopicPolicyOperation(TopicName topicName, PolicyName policy, PolicyOperation operation) {
        sync(()-> validateTopicPolicyOperationAsync(topicName, policy, operation));
    }

    public CompletableFuture<Void> validateTopicPolicyOperationAsync(TopicName topicName,
                                                                     PolicyName policy, PolicyOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(
                        new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request"));
            }
            return pulsar().getBrokerService().getAuthorizationService()
                    .allowTopicPolicyOperationAsync(topicName, policy, operation, originalPrincipal(), clientAppId(),
                            clientAuthData()).thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.FORBIDDEN,
                                    String.format("Unauthorized to validateTopicPolicyOperation"
                                            + " for operation [%s] on topic [%s] on policy [%s]", operation.toString(),
                                    topicName, policy.toString()));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> validateTopicOperationAsync(TopicName topicName, TopicOperation operation) {
       return validateTopicOperationAsync(topicName, operation, null);
    }

    public CompletableFuture<Void> validateTopicOperationAsync(TopicName topicName,
                                                               TopicOperation operation, String subscription) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(
                        new RestException(Status.UNAUTHORIZED, "Need to authenticate to perform the request"));
            }

            AuthenticationDataSource authData = clientAuthData();
            authData.setSubscription(subscription);
            return pulsar().getBrokerService().getAuthorizationService()
                    .allowTopicOperationAsync(topicName, operation, originalPrincipal(), clientAppId(), authData)
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED, String.format(
                                    "Unauthorized to validateTopicOperation for operation [%s] on topic [%s]",
                                    operation.toString(), topicName));
                        }
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public <T> T sync(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get().get(config().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (ExecutionException | TimeoutException ex) {
            Throwable realCause = FutureUtil.unwrapCompletionException(ex);
            if (realCause instanceof WebApplicationException) {
                throw (WebApplicationException) realCause;
            } else {
                throw new RestException(realCause);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RestException(ex);
        }
    }

    protected static void resumeAsyncResponseExceptionally(AsyncResponse asyncResponse, Throwable exception) {
        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
        if (realCause instanceof WebApplicationException) {
            asyncResponse.resume(realCause);
        } else if (realCause instanceof BrokerServiceException.NotAllowedException) {
            asyncResponse.resume(new RestException(Status.CONFLICT, realCause));
        } else if (realCause instanceof MetadataStoreException.NotFoundException) {
            asyncResponse.resume(new RestException(Status.NOT_FOUND, realCause));
        } else if (realCause instanceof MetadataStoreException.BadVersionException) {
            asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
        } else if (realCause instanceof PulsarAdminException) {
            asyncResponse.resume(new RestException(((PulsarAdminException) realCause)));
        } else {
            asyncResponse.resume(new RestException(realCause));
        }
    }

    protected String getClientVersion() {
        return httpRequest != null ? httpRequest.getHeader("User-Agent") : null;
    }
}
