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
package com.yahoo.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.yahoo.pulsar.common.naming.NamespaceBundleFactory.getBundlesData;
import static com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static java.lang.String.format;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.loadbalance.LoadManager;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.lookup.LookupResult;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.NamespaceIsolationPolicy;
import com.yahoo.pulsar.common.policies.data.BrokerAssignment;
import com.yahoo.pulsar.common.policies.data.BundlesData;
import com.yahoo.pulsar.common.policies.data.LocalPolicies;
import com.yahoo.pulsar.common.policies.data.NamespaceOwnershipStatus;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import com.yahoo.pulsar.common.util.Codec;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import static com.yahoo.pulsar.broker.web.PulsarWebResource.joinPath;

/**
 * The <code>NamespaceService</code> provides resource ownership lookup as well as resource ownership claiming services
 * for the <code>PulsarService</code>.
 * <p/>
 * The <code>PulsarService</code> relies on this service for resource ownership operations.
 * <p/>
 * The focus of this phase is to bring up the system and be able to iterate and improve the services effectively.
 * <p/>
 *
 * @see com.yahoo.pulsar.broker.PulsarService
 */
public class NamespaceService {

    public enum AddressType {
        BROKER_URL, LOOKUP_URL
    }

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);

    private final ServiceConfiguration config;

    private final LoadManager loadManager;

    private final PulsarService pulsar;

    private final OwnershipCache ownershipCache;

    private final NamespaceBundleFactory bundleFactory;

    private int uncountedNamespaces;

    private final String host;

    public static final String SLA_NAMESPACE_PROPERTY = "sla-monitor";
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN = Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    public static final Pattern SLA_NAMESPACE_PATTERN = Pattern.compile(SLA_NAMESPACE_PROPERTY + "/[^/]+/([^:]+:\\d+)");
    public static final String HEARTBEAT_NAMESPACE_FMT = "pulsar/%s/%s:%s";
    public static final String SLA_NAMESPACE_FMT = SLA_NAMESPACE_PROPERTY + "/%s/%s:%s";

    /**
     * Default constructor.
     *
     * @throws PulsarServerException
     */
    public NamespaceService(PulsarService pulsar) {
        this.pulsar = pulsar;
        host = pulsar.getHost();
        this.config = pulsar.getConfiguration();
        this.loadManager = pulsar.getLoadManager();
        ServiceUnitZkUtils.initZK(pulsar.getLocalZkCache().getZooKeeper(), pulsar.getBrokerServiceUrl());
        this.bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        this.ownershipCache = new OwnershipCache(pulsar, bundleFactory);
        LOG.info("namespace service is ready ...");
    }

    public LookupResult getBrokerServiceUrl(DestinationName fqdn, boolean authoritative) throws Exception {
        return findBrokerServiceUrl(getBundle(fqdn), authoritative, false);
    }

    public ServiceUnitId getBundle(DestinationName destination) throws Exception {
        return bundleFactory.getBundles(destination.getNamespaceObject()).findBundle(destination);
    }

    public int getBundleCount(NamespaceName namespace) throws Exception {
        return bundleFactory.getBundles(namespace).size();
    }

    private ServiceUnitId getFullBundle(NamespaceName fqnn) throws Exception {
        return bundleFactory.getFullBundle(fqnn);
    }

    public URL getWebServiceUrl(ServiceUnitId suName, boolean authoritative, boolean readOnly) throws Exception {
        if (suName instanceof DestinationName) {
            DestinationName name = (DestinationName) suName;
            LOG.debug("Getting web service URL of destination: {} - auth: {}", name, authoritative);

            return this.internalGetWebServiceUrl(getBundle(name), authoritative, readOnly);
        }

        if (suName instanceof NamespaceName) {
            return this.internalGetWebServiceUrl(getFullBundle((NamespaceName) suName), authoritative, readOnly);
        }

        if (suName instanceof NamespaceBundle) {
            return this.internalGetWebServiceUrl(suName, authoritative, readOnly);
        }

        throw new IllegalArgumentException("Unrecognized class of ServiceUnitId: " + suName.getClass().getName());
    }

    private URL internalGetWebServiceUrl(ServiceUnitId suName, boolean authoritative, boolean readOnly) {
        try {
            LookupResult result = findBrokerServiceUrl(suName, authoritative, readOnly);
            if (result != null) {
                if (result.isBrokerUrl()) {
                    // Somebody already owns the service unit
                    LookupData lookupData = result.getLookupData();
                    if (lookupData.getHttpUrl() != null) {
                        // If the broker uses the new format, we know the correct address
                        return new URL(lookupData.getHttpUrl());
                    } else {
                        // Fallback to use same port as current broker
                        URI brokerAddress = new URI(lookupData.getBrokerUrl());
                        String host = brokerAddress.getHost();
                        int port = config.getWebServicePort();
                        return new URL(String.format("http://%s:%s", host, port));
                    }
                } else {
                    // We have the HTTP address to redirect to
                    return result.getHttpRedirectAddress().toURL();
                }
            }
        } catch (Exception e) {
            // just log the exception, nothing else to do
            LOG.warn("internalGetWebServiceUrl [{}]", e.getMessage(), e);
        }

        return null;
    }

    /**
     * Register all the bootstrap name spaces including the heartbeat namespace
     *
     * @return
     * @throws PulsarServerException
     */
    public void registerBootstrapNamespaces() throws PulsarServerException {

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespace(host, config), true)) {
            this.uncountedNamespaces++;
            LOG.info("added heartbeat namespace name in local cache: ns={}", getHeartbeatNamespace(host, config));
        }

        // we may not need strict ownership checking for bootstrap names for now
        for (String namespace : config.getBootstrapNamespaces()) {
            if (registerNamespace(namespace, false)) {
                LOG.info("added bootstrap namespace name in local cache: ns={}", namespace);
            }
        }
    }

    /**
     * Tried to registers a namespace to this instance
     *
     * @param namespace
     * @param ensureOwned
     * @return
     * @throws PulsarServerException
     * @throws Exception
     */
    private boolean registerNamespace(String namespace, boolean ensureOwned) throws PulsarServerException {

        String myUrl = pulsar.getBrokerServiceUrl();

        try {
            NamespaceName nsname = new NamespaceName(namespace);

            // [Bug 6504511] Enable writing with JSON format
            String otherUrl = null;
            NamespaceBundle nsFullBundle = null;

            // all pre-registered namespace is assumed to have bundles disabled
            nsFullBundle = bundleFactory.getFullBundle(nsname);
            // v2 namespace will always use full bundle object
            otherUrl = ownershipCache.getOrSetOwner(nsFullBundle).getNativeUrl();

            if (myUrl.equals(otherUrl)) {
                if (nsFullBundle != null) {
                    // preload heartbeat namespace
                    pulsar.loadNamespaceDestinations(nsFullBundle);
                }
                return true;
            }

            String msg = String.format("namespace already owned by other broker : ns=%s expected=%s actual=%s",
                    namespace, myUrl, otherUrl);

            // ignore if not be owned for now
            if (!ensureOwned) {
                LOG.info(msg);
                return false;
            }

            // should not happen
            throw new IllegalStateException(msg);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        }
    }

    /**
     * Main internal method to lookup and setup ownership of service unit to a broker
     *
     * @param suName
     * @param authoritative
     * @param readOnly
     * @return
     * @throws PulsarServerException
     */
    private LookupResult findBrokerServiceUrl(ServiceUnitId suName, boolean authoritative, boolean readOnly)
            throws PulsarServerException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("findBrokerServiceUrl: {} - read-only: {}", suName, readOnly);
        }
        // First do a read-only lookup for the ownership info
        try {
            NamespaceEphemeralData ownerInfo = checkNotNull(ownershipCache.getOwner(suName));
            if (ownerInfo.isDisabled()) {
                throw new IllegalStateException(String.format("ServiceUnit %s is tentatively out-of-service.", suName));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("SU {} already owned by {} ", suName, ownerInfo.toString());
            }
            return new LookupResult(ownerInfo);
        } catch (NoNodeException nne) {
            // no owner ship found in the cache
            LOG.debug("NoNodeException ", nne);
        } catch (NullPointerException npe) {
            // no owner ship found in the cache
            LOG.debug("NullPointerException ", npe);
        } catch (IllegalStateException ise) {
            LOG.warn("ServiceUnit {} is tentatively out-of-service.", suName);
            throw ise;
        } catch (Exception e) {
            LOG.error("Failed to get ownership info from ZooKeeper cache for ServiceUnit {}", suName);
            throw new PulsarServerException(e);
        }

        if (readOnly) {
            // This lookup is a read-only call. If not found, just throw exception out
            throw new IllegalStateException(String.format("Can't find owner of ServiceUnit: %s", suName));
        }

        // Now, no one owns the namespace yet. Hence, we will try to dynamically assign it
        String candidateBroker = null;
        try {
            // check if this is Heartbeat or SLAMonitor namespace
            candidateBroker = checkHeartbeatNamespace(suName);
            if (candidateBroker == null) {
                String broker = getSLAMonitorBrokerName(suName);
                // checking if the broker is up and running
                if (broker != null && isBrokerActive(broker)) {
                    candidateBroker = broker;
                }
            }

            if (candidateBroker == null) {
                if (!this.loadManager.isCentralized() || pulsar.getLeaderElectionService().isLeader()) {
                    candidateBroker = getLeastLoadedFromLoadManager(suName);
                } else {
                    if (authoritative) {
                        // leader broker already assigned the current broker as owner
                        candidateBroker = pulsar.getWebServiceAddress();
                    } else {
                        // forward to leader broker to make assignment
                        candidateBroker = pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl();
                    }
                }
            }
        } catch (IllegalStateException ise) {
            // The error has already been logged.
            throw ise;
        } catch (Exception oe) {
            LOG.warn(String.format("Cannot find candidate broker for ServiceUnit %s in findBrokerServiceUrl:[%s]",
                    suName, oe.getMessage()), oe);
        }
        checkNotNull(candidateBroker);

        try {
            if (pulsar.getWebServiceAddress().equals(candidateBroker)) {
                // Load manager decided that the local broker should be the owner. Acquiring the ownership
                NamespaceEphemeralData ownerInfo = checkNotNull(ownershipCache.getOrSetOwner(suName));
                if (ownerInfo.isDisabled()) {
                    LOG.warn("ServiceUnit {} is tentatively out-of-service", suName);
                    throw new IllegalStateException(
                            String.format("ServiceUnit %s is tentatively out-of-service", suName));
                }
                // schedule the task to pre-load destinations
                pulsar.loadNamespaceDestinations(suName);

                // Now, whatever returned in the ownerInfo is the owner of the namespace
                return new LookupResult(ownerInfo);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "My BrokerServiceUrl{}, WebServiceAddress[{}] : other instance owns the namespace, owner "
                                    + "address={} suName={}",
                            pulsar.getBrokerServiceUrl(), pulsar.getWebServiceAddress(), candidateBroker, suName);
                }
                // Now setting the redirect url
                return new LookupResult(new URI(candidateBroker));
            }
        } catch (IllegalStateException ise) {
            // already logged the exception
            throw ise;
        } catch (Exception e2) {
            // in this case addresses should be Null so we reply on NPE thrown by checkNotNull
            LOG.warn(String.format("Failed to acquire the ServiceUnit %s in findBrokerServiceUrl:[%s]", suName,
                    e2.getMessage()), e2);
            throw new PulsarServerException(e2);
        }
    }

    private boolean isBrokerActive(String candidateBroker) throws KeeperException, InterruptedException {
        Set<String> activeNativeBrokers = pulsar.getLocalZkCache()
                .getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT);

        for (String brokerHostPort : activeNativeBrokers) {
            if (candidateBroker.equals("http://" + brokerHostPort)) {
                LOG.debug("Broker {} found for SLA Monitoring Namespace", brokerHostPort);
                return true;
            }
        }
        LOG.debug("Broker not found for SLA Monitoring Namespace {}",
                candidateBroker + ":" + config.getWebServicePort());
        return false;
    }

    /**
     * Helper function to encapsulate the logic to invoke between old and new load manager
     *
     * @param namespaceName
     * @param decidedByLeader
     * @return
     * @throws Exception
     */
    private String getLeastLoadedFromLoadManager(ServiceUnitId serviceUnit) throws Exception {
        String lookupAddress = loadManager.getLeastLoaded(serviceUnit).getResourceId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} : redirecting to the least loaded broker, lookup address={}", pulsar.getWebServiceAddress(),
                    lookupAddress);
        }
        return lookupAddress;
    }

    public void unloadNamespace(NamespaceName ns) throws Exception {
        ServiceUnitId nsFullBundle = getFullBundle(ns);
        unloadServiceUnit(nsFullBundle);
    }

    public void unloadNamespaceBundle(NamespaceBundle nsBundle) throws Exception {
        unloadServiceUnit(nsBundle);
    }

    private void unloadServiceUnit(ServiceUnitId suName) throws Exception {
        checkNotNull(ownershipCache.getOwnedServiceUnit(suName)).handleUnloadRequest(pulsar);
    }

    public Map<String, NamespaceOwnershipStatus> getOwnedNameSpacesStatus() throws Exception {
        NamespaceIsolationPolicies nsIsolationPolicies = this.getLocalNamespaceIsolationPolicies();
        Map<String, NamespaceOwnershipStatus> ownedNsStatus = new HashMap<String, NamespaceOwnershipStatus>();
        for (OwnedServiceUnit nsObj : this.ownershipCache.getOwnedServiceUnits().values()) {
            NamespaceOwnershipStatus nsStatus = this.getNamespaceOwnershipStatus(nsObj,
                    nsIsolationPolicies.getPolicyByNamespace(nsObj.getServiceUnitId().getNamespaceObject()));
            ownedNsStatus.put(nsObj.getServiceUnitId().toString(), nsStatus);
        }

        return ownedNsStatus;
    }

    private NamespaceOwnershipStatus getNamespaceOwnershipStatus(OwnedServiceUnit nsObj,
            NamespaceIsolationPolicy nsIsolationPolicy) {
        NamespaceOwnershipStatus nsOwnedStatus = new NamespaceOwnershipStatus(BrokerAssignment.shared, false,
                nsObj.isActive());
        if (nsIsolationPolicy == null) {
            // no matching policy found, this namespace must be an uncontrolled one and using shared broker
            return nsOwnedStatus;
        }
        // found corresponding policy, set the status to controlled
        nsOwnedStatus.is_controlled = true;
        if (nsIsolationPolicy.isPrimaryBroker(pulsar.getHost())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.primary;
        } else if (nsIsolationPolicy.isSecondaryBroker(pulsar.getHost())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.secondary;
        }

        return nsOwnedStatus;
    }

    private NamespaceIsolationPolicies getLocalNamespaceIsolationPolicies() throws Exception {
        try {
            String localCluster = pulsar.getConfiguration().getClusterName();
            return pulsar.getConfigurationCache().namespaceIsolationPoliciesCache()
                    .get(AdminResource.path("clusters", localCluster, "namespaceIsolationPolicies"));
        } catch (KeeperException.NoNodeException nne) {
            // the namespace isolation policies are empty/undefined = an empty object
            return new NamespaceIsolationPolicies();
        }
    }

    public boolean isServiceUnitDisabled(ServiceUnitId suName) throws Exception {
        checkArgument(suName instanceof NamespaceName || suName instanceof NamespaceBundle,
                "Only support NamespaceName or NamespaceBundle in service unit ownership");

        ServiceUnitId serviceUnit = null;
        if (suName instanceof NamespaceName) {
            serviceUnit = getFullBundle(suName.getNamespaceObject());
        }
        if (suName instanceof NamespaceBundle) {
            serviceUnit = suName;
        }
        checkNotNull(serviceUnit);

        try {
            // Does ZooKeeper says that the namespace is disabled?
            return checkNotNull(ownershipCache.getOwner(serviceUnit)).isDisabled();
        } catch (NullPointerException npe) {
            // if namespace is not owned, it is not considered disabled
            return false;
        } catch (NoNodeException nne) {
            // if no node exists, that means the namespace is not owned
            return false;
        } catch (Exception e) {
            LOG.warn(String.format("Exception in getting ownership info for service unit %s", serviceUnit), e);
        }

        return false;
    }

    /**
     * 1. split the given bundle into two bundles 2. assign ownership of both the bundles to current broker 3. update
     * policies with newly created bundles into LocalZK 4. disable original bundle and refresh the cache
     *
     * @param bundle
     * @return
     * @throws Exception
     */
    public CompletableFuture<Void> splitAndOwnBundle(NamespaceBundle bundle) throws Exception {

        final CompletableFuture<Void> future = new CompletableFuture<>();

        Pair<NamespaceBundles, List<NamespaceBundle>> splittedBundles = bundleFactory.splitBundles(bundle,
                2 /* by default split into 2 */);
        if (splittedBundles != null) {
            checkNotNull(splittedBundles.getLeft());
            checkNotNull(splittedBundles.getRight());
            checkArgument(splittedBundles.getRight().size() == 2, "bundle has to be split in two bundles");
            NamespaceName nsname = bundle.getNamespaceObject();
            try {
                // take ownership of newly split bundles
                for (NamespaceBundle sBundle : splittedBundles.getRight()) {
                    checkNotNull(ownershipCache.getOrSetOwner(sBundle));
                }
                updateNamespaceBundles(nsname, splittedBundles.getLeft(), new StatCallback() {
                    public void processResult(int rc, String path, Object zkCtx, Stat stat) {
                        if (rc == KeeperException.Code.OK.intValue()) {
                            // disable old bundle
                            try {
                                ownershipCache.disableOwnership(bundle);
                                // invalidate cache as zookeeper has new split
                                // namespace bundle
                                bundleFactory.invalidateBundleCache(nsname);
                                // update bundled_topic cache for load-report-generation
                                pulsar.getBrokerService().refreshTopicToStatsMaps(bundle);
                                loadManager.setLoadReportForceUpdateFlag();
                                future.complete(null);
                            } catch (Exception e) {
                                String msg = format("failed to disable bundle %s under namespace [%s] with error %s",
                                        nsname.toString(), bundle.toString(), e.getMessage());
                                LOG.warn(msg, e);
                                future.completeExceptionally(new ServiceUnitNotReadyException(msg));
                            }
                        } else {
                            String msg = format("failed to update namespace [%s] policies due to %s", nsname.toString(),
                                    KeeperException.create(KeeperException.Code.get(rc)).getMessage());
                            LOG.warn(msg);
                            future.completeExceptionally(new ServiceUnitNotReadyException(msg));
                        }
                    }
                });
            } catch (Exception e) {
                String msg = format("failed to aquire ownership of split bundle for namespace [%s], %s",
                        nsname.toString(), e.getMessage());
                LOG.warn(msg, e);
                future.completeExceptionally(new ServiceUnitNotReadyException(msg));
            }

        } else {
            String msg = format("bundle %s not found under namespace", bundle.toString());
            future.completeExceptionally(new ServiceUnitNotReadyException(msg));
        }
        return future;
    }

    /**
     * update new bundle-range to LocalZk (create a new node if not present)
     *
     * @param nsname
     * @param nsBundles
     * @param callback
     * @throws Exception
     */
    private void updateNamespaceBundles(NamespaceName nsname, NamespaceBundles nsBundles, StatCallback callback)
            throws Exception {
        checkNotNull(nsname);
        checkNotNull(nsBundles);
        String path = joinPath(LOCAL_POLICIES_ROOT, nsname.toString());
        LocalPolicies policies = null;
        try {
            policies = this.pulsar.getLocalZkCacheService().policiesCache().get(path);
        } catch (KeeperException.NoNodeException ne) {
            // if policies is not present into localZk then create new policies
            this.pulsar.getLocalZkCacheService().createPolicies(path, false);
            policies = this.pulsar.getLocalZkCacheService().policiesCache().get(path);
        }
        policies.bundles = getBundlesData(nsBundles);
        this.pulsar.getLocalZkCache().getZooKeeper().setData(path,
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies), -1, callback, null);
    }

    public OwnershipCache getOwnershipCache() {
        return ownershipCache;
    }

    public int getTotalServiceUnitsLoaded() {
        return ownershipCache.getOwnedServiceUnits().size() - this.uncountedNamespaces;
    }
    
    public Set<ServiceUnitId> getOwnedServiceUnits() {
        return ownershipCache.getOwnedServiceUnits().values().stream().map(su -> {
            return su.getServiceUnitId();
        }).collect(Collectors.toSet());
    }

    public boolean isServiceUnitOwned(ServiceUnitId suName) throws Exception {
        if (suName instanceof DestinationName) {
            return isDestinationOwned((DestinationName) suName);
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwned((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            return ownershipCache.getOwnedServiceUnit(suName) != null;
        }

        throw new IllegalArgumentException("Invalid class of ServiceUnitId: " + suName.getClass().getName());
    }

    public boolean isServiceUnitActive(DestinationName fqdn) {
        try {
            return ownershipCache.getOwnedServiceUnit(getBundle(fqdn)).isActive();
        } catch (Exception e) {
            LOG.warn("Unable to find OwnedServiceUnit for fqdn - [{}]", fqdn.toString());
            return false;
        }
    }

    private boolean isNamespaceOwned(NamespaceName fqnn) throws Exception {
        return ownershipCache.getOwnedServiceUnit(getFullBundle(fqnn)) != null;
    }

    private boolean isDestinationOwned(DestinationName fqdn) throws Exception {
        return ownershipCache.getOwnedServiceUnit(getBundle(fqdn)) != null;
    }

    public void removeOwnedServiceUnit(NamespaceName nsName) throws Exception {
        ownershipCache.removeOwnership(getFullBundle(nsName));
    }

    public void removeOwnedServiceUnit(NamespaceBundle nsBundle) throws Exception {
        ownershipCache.removeOwnership(nsBundle);
    }

    public void removeOwnedServiceUnits(NamespaceName nsName, BundlesData bundleData) throws Exception {
        ownershipCache.removeOwnership(bundleFactory.getBundles(nsName, bundleData));
    }

    public NamespaceBundleFactory getNamespaceBundleFactory() {
        return bundleFactory;
    }

    public ServiceUnitId getServiceUnitId(DestinationName destinationName) throws Exception {
        return getBundle(destinationName);
    }

    public List<String> getListOfDestinations(String property, String cluster, String namespace) throws Exception {
        List<String> destinations = Lists.newArrayList();

        // For every topic there will be a managed ledger created.
        try {
            String path = String.format("/managed-ledgers/%s/%s/%s/persistent", property, cluster, namespace);
            LOG.debug("Getting children from managed-ledgers now: {}", path);
            for (String destination : pulsar.getLocalZkCacheService().managedLedgerListCache().get(path)) {
                destinations.add(String.format("persistent://%s/%s/%s/%s", property, cluster, namespace,
                        Codec.decode(destination)));
            }
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no persistent topics for this namespace
        }

        Collections.sort(destinations);
        return destinations;
    }

    public NamespaceEphemeralData getOwner(ServiceUnitId suname) throws Exception {
        try {
            return ownershipCache.getOwner(suname);
        } catch (KeeperException.NoNodeException e) {
            // if there is no znode for the service unit, it is not owned by any broker
            return null;
        }
    }

    public void unloadSLANamespace() throws Exception {
        PulsarAdmin adminClient = null;
        String namespaceName = getSLAMonitorNamespace(host, config);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to unload SLA namespace {}", namespaceName);
        }

        ServiceUnitId nsFullBundle = getFullBundle(new NamespaceName(namespaceName));
        if (getOwner(nsFullBundle) == null) {
            // No one owns the namespace so no point trying to unload it
            return;
        }
        adminClient = pulsar.getAdminClient();
        adminClient.namespaces().unload(namespaceName);
        LOG.debug("Namespace {} unloaded successfully", namespaceName);
    }

    public static String getHeartbeatNamespace(String host, ServiceConfiguration config) {
        return String.format(HEARTBEAT_NAMESPACE_FMT, config.getClusterName(), host, config.getWebServicePort());
    }

    public static String getSLAMonitorNamespace(String host, ServiceConfiguration config) {
        return String.format(SLA_NAMESPACE_FMT, config.getClusterName(), host, config.getWebServicePort());
    }

    public static String checkHeartbeatNamespace(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("SLAMonitoring namespace matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static String getSLAMonitorBrokerName(ServiceUnitId ns) {
        Matcher m = SLA_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public boolean registerSLANamespace() throws PulsarServerException {
        boolean isNameSpaceRegistered = registerNamespace(getSLAMonitorNamespace(host, config), false);
        if (isNameSpaceRegistered) {
            this.uncountedNamespaces++;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Added SLA Monitoring namespace name in local cache: ns={}",
                        getSLAMonitorNamespace(host, config));
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("SLA Monitoring not owned by the broker: ns={}", getSLAMonitorNamespace(host, config));
        }
        return isNameSpaceRegistered;
    }
}
