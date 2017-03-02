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
package com.yahoo.pulsar.broker.web;

import static com.google.common.base.Preconditions.checkArgument;
import static com.yahoo.pulsar.common.api.Commands.newLookupResponse;

import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.admin.Namespaces;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.BundlesData;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;

/**
 * Base class for Web resources in Pulsar. It provides basic authorization functions.
 */
public abstract class PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(PulsarWebResource.class);

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
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public static String joinPath(String... parts) {
        StringBuilder sb = new StringBuilder();
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public static String splitPath(String source, int slice) {
        Iterable<String> parts = Splitter.on('/').limit(slice).split(source);
        Iterator<String> s = parts.iterator();
        String result = new String();
        for (int i = 0; i < slice; i++) {
            result = s.next();
        }
        return result;
    }

    /**
     * Gets a caller id (IP + role)
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        return (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    }
    
    public boolean isRequestHttps() {
    	return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    public static boolean isClientAuthenticated(String appId) {
        return appId != null;
    }

    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws WebApplicationException
     *             if not authorized
     */
    protected void validateSuperUserAccess() {
        if (config().isAuthenticationEnabled()) {
            String appId = clientAppId();
            if(log.isDebugEnabled()) {
                log.debug("[{}] Check super user access: Authenticated: {} -- Role: {}", uri.getRequestUri(),
                        isClientAuthenticated(appId), appId);                
            }
            if (!config().getSuperUserRoles().contains(appId)) {
                throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
            }
        }
    }

    /**
     * Checks that the http client role has admin access to the specified property.
     *
     * @param property
     *            the property id
     * @throws WebApplicationException
     *             if not authorized
     */
    protected void validateAdminAccessOnProperty(String property) {
        validateAdminAccessOnProperty(pulsar(), clientAppId(), property);
    }
    
    protected static void validateAdminAccessOnProperty(PulsarService pulsar, String clientAppId, String property) {
        if (pulsar.getConfiguration().isAuthenticationEnabled() && pulsar.getConfiguration().isAuthorizationEnabled()) {
            log.debug("check admin access on property: {} - Authenticated: {} -- role: {}", property,
                    (isClientAuthenticated(clientAppId)), clientAppId);

            if (!isClientAuthenticated(clientAppId)) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            if (pulsar.getConfiguration().getSuperUserRoles().contains(clientAppId)) {
                // Super-user has access to configure all the policies
                log.debug("granting access to super-user {} on property {}", clientAppId, property);
            } else {
                PropertyAdmin propertyAdmin;

                try {
                    propertyAdmin = pulsar.getConfigurationCache().propertiesCache().get(path("policies", property))
                            .orElseThrow(() -> new RestException(Status.UNAUTHORIZED, "Property does not exist"));
                } catch (KeeperException.NoNodeException e) {
                    log.warn("Failed to get property admin data for non existing property {}", property);
                    throw new RestException(Status.UNAUTHORIZED, "Property does not exist");
                } catch (Exception e) {
                    log.error("Failed to get property admin data for property");
                    throw new RestException(e);
                }

                if (!propertyAdmin.getAdminRoles().contains(clientAppId)) {
                    throw new RestException(Status.UNAUTHORIZED,
                            "Don't have permission to administrate resources on this property");
                }

                log.debug("Successfully authorized {} on property {}", clientAppId, property);
            }
        }
    }

    protected void validateClusterForProperty(String property, String cluster) {
        PropertyAdmin propertyAdmin;
        try {
            propertyAdmin = pulsar().getConfigurationCache().propertiesCache().get(path("policies", property))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Property does not exist"));
        } catch (Exception e) {
            log.error("Failed to get property admin data for property");
            throw new RestException(e);
        }

        // Check if property is allowed on the cluster
        if (!propertyAdmin.getAllowedClusters().contains(cluster)) {
            String msg = String.format("Cluster [%s] is not in the list of allowed clusters list for property [%s]",
                    cluster, property);
            log.info(msg);
            throw new RestException(Status.FORBIDDEN, msg);
        }
        log.info("Successfully validated clusters on property [{}]", property);
    }

    /**
     * Check if the cluster exists and redirect the call to the owning cluster
     *
     * @param cluster
     *            Cluster name
     * @throws Exception
     *             In case the redirect happens
     */
    protected void validateClusterOwnership(String cluster) throws WebApplicationException {
        try {
            ClusterData differentClusterData = getClusterDataIfDifferentCluster(pulsar(), cluster, clientAppId()).get();
            if (differentClusterData != null) {
                URL webUrl;
                if (pulsar.getConfiguration().isTlsEnabled() && !differentClusterData.getServiceUrlTls().isEmpty()) {
                    webUrl = new URL(differentClusterData.getServiceUrlTls());
                } else {
                    webUrl = new URL(differentClusterData.getServiceUrl());
                }
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort())
                        .build();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, cluster);

                }
                // redirect to the cluster requested
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
    
    protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
            String cluster, String clientAppId) {

        CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();

        if (!isValidCluster(pulsar, cluster)) {
            try {
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

    protected static boolean isValidCluster(PulsarService pulsarSevice, String cluster) {// If the cluster name is
                                                                                         // "global", don't validate the
                                                                                         // cluster ownership.
        // The validation will be done by checking the namespace configuration
        if (cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            return true;
        }

        if (!pulsarSevice.getConfiguration().isAuthorizationEnabled()) {
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
     * @param fqnn
     * @param authoritative
     * @param readOnly
     * @param bundleData
     */
    protected void validateNamespaceOwnershipWithBundles(String property, String cluster, String namespace,
            boolean authoritative, boolean readOnly, BundlesData bundleData) {
        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);

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
            log.debug(String.format("Failed to find owner for namespace %s", fqnn), oe);
            throw new RestException(oe);
        }
    }

    protected void validateBundleOwnership(String property, String cluster, String namespace, boolean authoritative,
            boolean readOnly, NamespaceBundle bundle) {
        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);

        try {
            validateBundleOwnership(bundle, authoritative, readOnly);
        } catch (WebApplicationException wae) {
            // propagate already wrapped-up WebApplicationExceptions
            throw wae;
        } catch (Exception oe) {
            log.debug(String.format("Failed to find owner for namespace %s", fqnn), oe);
            throw new RestException(oe);
        }
    }

    protected NamespaceBundle validateNamespaceBundleRange(NamespaceName fqnn, BundlesData bundles,
            String bundleRange) {
        try {
            checkArgument(bundleRange.contains("_"), "Invalid bundle range");
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
        } catch (Exception e) {
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
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
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
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
            URL webUrl = nsService.getWebServiceUrl(bundle, authoritative, isRequestHttps(), readOnly);
            // Ensure we get a url
            if (webUrl == null) {
                log.warn("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Failed to find ownership for ServiceUnit:" + bundle.toString());
            }

            if (!nsService.isServiceUnitOwned(bundle)) {
                boolean newAuthoritative = this.isLeaderBroker();
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort())
                        .replaceQueryParam("authoritative", newAuthoritative).build();

                log.debug("{} is not a service unit owned", bundle);

                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug(String.format("Failed to find owner for ServiceUnit %s", bundle), iae);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "ServiceUnit format is not expected. ServiceUnit " + bundle);
        } catch (IllegalStateException ise) {
            log.debug(String.format("Failed to find owner for ServiceUnit %s", bundle), ise);
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
     * @param authoritative
     *
     * @param property
     * @param cluster
     * @param namespace
     */
    protected void validateDestinationOwnership(DestinationName fqdn, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        try {
            // per function name, this is trying to acquire the whole namespace ownership
            URL webUrl = nsService.getWebServiceUrl(fqdn, authoritative, isRequestHttps(), false);
            // Ensure we get a url
            if (webUrl == null) {
                log.info("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for destination:" + fqdn);
            }

            if (!nsService.isServiceUnitOwned(fqdn)) {
                boolean newAuthoritative = this.isLeaderBroker(pulsar());
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort())
                        .replaceQueryParam("authoritative", newAuthoritative).build();
                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug(String.format("Failed to find owner for destination:%s", fqdn), iae);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for destination " + fqdn);
        } catch (IllegalStateException ise) {
            log.debug(String.format("Failed to find owner for destination:%s", fqdn), ise);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for destination " + fqdn);
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception oe) {
            log.debug(String.format("Failed to find owner for destination:%s", fqdn), oe);
            throw new RestException(oe);
        }
    }

    protected void validateReplicationSettingsOnNamespace(String property, String cluster, String namespace) {
        NamespaceName namespaceName = new NamespaceName(property, cluster, namespace);
        validateReplicationSettingsOnNamespace(pulsar(), namespaceName);
    }

    /**
     * If the namespace is global, validate the following - 1. If replicated clusters are configured for this global
     * namespace 2. If local cluster belonging to this namespace is replicated 3. If replication is enabled for this
     * namespace
     *
     * @param pulsarService
     * @param namespace
     * @throws Exception
     */
    protected static void validateReplicationSettingsOnNamespace(PulsarService pulsarService, NamespaceName namespace) {
        try {
            validateReplicationSettingsOnNamespaceAsync(pulsarService, namespace).get();
        } catch (Exception e) {
            if(e.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) e.getCause();
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        }
    }
    
    protected static CompletableFuture<Void> validateReplicationSettingsOnNamespaceAsync(PulsarService pulsarService,
            NamespaceName namespace) {

        CompletableFuture<Void> validationFuture = new CompletableFuture<>();

        if (namespace.isGlobal()) {
            String localCluster = pulsarService.getConfiguration().getClusterName();

            String path = AdminResource.path("policies", namespace.getProperty(), namespace.getCluster(),
                    namespace.getLocalName());

            pulsarService.getConfigurationCache().policiesCache().getAsync(path).thenAccept(policiesResult -> {

                if (policiesResult.isPresent()) {
                    Policies policies = policiesResult.get();
                    if (policies.replication_clusters.isEmpty()) {
                        String msg = String.format(
                                "Global namespace does not have any clusters configured : local_cluster=%s ns=%s",
                                localCluster, namespace.toString());
                        log.warn(msg);
                        validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                    } else if (!policies.replication_clusters.contains(localCluster)) {
                        String msg = String.format(
                                "Global namespace missing local cluster name in replication list : local_cluster=%s ns=%s repl_clusters=%s",
                                localCluster, namespace.toString(), policies.replication_clusters);

                        log.warn(msg);
                        // TODO: when we have a fail-over policy defined, we should find the next cluster in the
                        // replication
                        // clusters to re-direct the request to
                        validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                    } else {
                        validationFuture.complete(null);
                    }

                } else {
                    String msg = String.format("Policies not found for %s namespace", namespace.toString());
                    log.error(msg);
                    validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND, msg));
                }

            }).exceptionally(ex -> {
                String msg = String.format(
                        "Failed to validate global cluster configuration : cluster=%s ns=%s  emsg=%s", localCluster,
                        namespace, ex.getMessage());
                log.error(msg);
                validationFuture.completeExceptionally(new RestException(ex));
                return null;
            });

        } else {
            validationFuture.complete(null);
        }

        return validationFuture;
    }

    protected void checkConnect(DestinationName destination) throws RestException, Exception {
        checkAuthorization(pulsar(), destination, clientAppId());
    }
    
    protected static void checkAuthorization(PulsarService pulsarService, DestinationName destination, String role) throws RestException, Exception{
        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // No enforcing of authorization policies
            return;
        }
        try {
            // get zk policy manager
            if (!pulsarService.getBrokerService().getAuthorizationManager().canLookup(destination, role)) {
                log.warn("[{}] Role {} is not allowed to lookup topic", destination, role);
                throw new RestException(Status.UNAUTHORIZED, "Don't have permission to connect to this namespace");
            }
        } catch (RestException e) {
            // Let it through
            throw e;
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

        String leaderAddress = pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl();

        String myAddress = pulsar.getWebServiceAddress();

        return myAddress.equals(leaderAddress); // If i am the leader, my decisions are
    }

    // Non-Usual HTTP error codes
    protected static final int NOT_IMPLEMENTED = 501;

}
