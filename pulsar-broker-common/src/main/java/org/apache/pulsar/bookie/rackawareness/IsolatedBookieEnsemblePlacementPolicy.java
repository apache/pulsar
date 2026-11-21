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
package org.apache.pulsar.bookie.rackawareness;

import static java.util.Collections.emptySet;
import static org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping.METADATA_STORE_INSTANCE;
import static org.apache.pulsar.bookie.rackawareness.BookieRackConfigurationSnapshot.RACK_CONFIGURATION_SNAPSHOT;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.util.HashedWheelTimer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;

@CustomLog
public class IsolatedBookieEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    public static final String ISOLATION_BOOKIE_GROUPS = "isolationBookieGroups";
    public static final String SECONDARY_ISOLATION_BOOKIE_GROUPS = "secondaryIsolationBookieGroups";

    // Using a pair to save the isolation groups, the left value is the primary group and the right value is
    // the secondary group.
    private ImmutablePair<Set<String>, Set<String>> defaultIsolationGroups;

    private static final String PULSAR_SYSTEM_TOPIC_ISOLATION_GROUP = "*";
    private BookieRackConfigurationSnapshot snapshot;
    private boolean ownsSnapshot;

    /** Completes when the initial rack configuration is applied or fails with the read error. */
    @Getter
    @VisibleForTesting
    private volatile CompletableFuture<Void> initialRackConfigurationLoadFuture =
            CompletableFuture.completedFuture(null);

    public IsolatedBookieEnsemblePlacementPolicy() {
        super();
    }

    @Override
    public RackawareEnsemblePlacementPolicyImpl initialize(ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer timer, FeatureProvider featureProvider,
            StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {
        // Without default isolation groups, ledgers that carry no isolation groups in their metadata are not isolated
        defaultIsolationGroups = null;
        if (conf.getProperty(ISOLATION_BOOKIE_GROUPS) != null) {
            Set<String> primaryIsolationGroups = new HashSet<>();
            Set<String> secondaryIsolationGroups = new HashSet<>();
            String isolationGroupsString = ConfigurationStringUtil
                    .castToString(conf.getProperty(ISOLATION_BOOKIE_GROUPS));
            if (!isolationGroupsString.isEmpty()) {
                Collections.addAll(primaryIsolationGroups, isolationGroupsString.split(","));
            }
            if (conf.getProperty(SECONDARY_ISOLATION_BOOKIE_GROUPS) != null) {
                String secondaryIsolationGroupsString = ConfigurationStringUtil
                        .castToString(conf.getProperty(SECONDARY_ISOLATION_BOOKIE_GROUPS));
                if (!secondaryIsolationGroupsString.isEmpty()) {
                    Collections.addAll(secondaryIsolationGroups, secondaryIsolationGroupsString.split(","));
                }
            }
            defaultIsolationGroups = ImmutablePair.of(primaryIsolationGroups, secondaryIsolationGroups);
        }
        if (conf.getProperty(RACK_CONFIGURATION_SNAPSHOT) instanceof BookieRackConfigurationSnapshot sharedSnapshot) {
            snapshot = sharedSnapshot;
            ownsSnapshot = false;
        } else {
            try {
                MetadataStore store = BookieRackAffinityMapping.getMetadataStore(conf);
                // Standalone policies have no owner to manage a background refresh lifecycle.
                snapshot = new BookieRackConfigurationSnapshot(store, conf.getZkTimeout());
                ownsSnapshot = true;
            } catch (MetadataException e) {
                throw new RuntimeException(METADATA_STORE_INSTANCE + " failed initialized", e);
            }
        }
        // Per-ledger configuration can require isolation on a client with no default groups.
        initialRackConfigurationLoadFuture = ownsSnapshot ? snapshot.refresh().copy() : snapshot.initialLoad();
        try {
            return super.initialize(conf, optionalDnsResolver, timer, featureProvider, statsLogger,
                    bookieAddressResolver);
        } catch (RuntimeException | Error e) {
            if (ownsSnapshot) {
                snapshot.close();
            }
            throw e;
        }
    }

    @Override
    public void uninitalize() {
        try {
            super.uninitalize();
        } finally {
            if (ownsSnapshot) {
                snapshot.close();
            }
        }
    }

    @VisibleForTesting
    public MetadataCache<BookiesRackConfiguration> getBookieMappingCache() {
        return snapshot == null ? null : snapshot.cache();
    }

    /**
     * Awaits rack configuration for a placement using this client. Each caller has its own deadline.
     */
    public CompletableFuture<Void> awaitRackConfiguration(EnsemblePlacementPolicyConfig placementConfig,
                                                           long timeoutMillis) {
        if (snapshot.latest() != null) {
            return CompletableFuture.completedFuture(null);
        }
        return FutureUtil.supplySafely(() -> {
            var groups = getEffectiveIsolationGroups(placementConfig);
            if (groups == null || isWildcard(groups)) {
                return CompletableFuture.completedFuture(null);
            }
            return snapshot.await(timeoutMillis);
        });
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        if (excludeBookies == null) {
            excludeBookies = new HashSet<>();
        }
        // Keep isolation filtering and BookKeeper selection on the same topology view.
        rwLock.readLock().lock();
        try {
            excludeBookies.addAll(getExcludedBookies(ensembleSize, customMetadata));
            return super.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        if (excludeBookies == null) {
            excludeBookies = new HashSet<>();
        }
        rwLock.readLock().lock();
        try {
            excludeBookies.addAll(getExcludedBookies(ensembleSize, customMetadata));
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, currentEnsemble,
                    bookieToReplace, excludeBookies);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private Set<BookieId> getExcludedBookies(int ensembleSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        // parse the ensemble placement policy from the custom metadata, if it is present, we will apply it to
        // the isolation groups for filtering the bookies.
        var groups = getEffectiveIsolationGroups(getEnsemblePlacementPolicyConfig(customMetadata).orElse(null));
        if (groups == null) {
            // Neither the ledger metadata nor the client configuration defines isolation groups
            return emptySet();
        }
        return getExcludedBookiesWithIsolationGroups(ensembleSize, groups);
    }

    private Pair<Set<String>, Set<String>> getEffectiveIsolationGroups(EnsemblePlacementPolicyConfig config) {
        return config == null || config.getPolicyClass() == null ? defaultIsolationGroups : getIsolationGroup(config);
    }

    private static boolean isWildcard(Pair<Set<String>, Set<String>> groups) {
        return groups.getLeft().contains(PULSAR_SYSTEM_TOPIC_ISOLATION_GROUP);
    }

    private static Optional<EnsemblePlacementPolicyConfig> getEnsemblePlacementPolicyConfig(
        Map<String, byte[]> customMetadata) {

        byte[] ensemblePlacementPolicyConfigData = customMetadata.get(
            EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        if (ensemblePlacementPolicyConfigData != null) {
            try {
                return Optional.ofNullable(EnsemblePlacementPolicyConfig.decode(ensemblePlacementPolicyConfigData));
            } catch (EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException e) {
                log.error()
                        .exception(e)
                        .log("Failed to parse the ensemble placement policy config from the custom metadata");
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    Pair<Set<String>, Set<String>> getIsolationGroup(
            EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) {
        // Retain compatibility with ZkIsolatedBookieEnsemblePlacementPolicy
        Class<?> policyClass = ensemblePlacementPolicyConfig.getPolicyClass();
        if (IsolatedBookieEnsemblePlacementPolicy.class.isAssignableFrom(policyClass)) {
            Map<String, Object> properties = ensemblePlacementPolicyConfig.getProperties();
            if (properties == null) {
                return defaultIsolationGroups;
            }
            MutablePair<Set<String>, Set<String>> pair = new MutablePair<>(emptySet(), emptySet());
            String primaryIsolationGroupString = ConfigurationStringUtil
                    .castToString(properties.getOrDefault(ISOLATION_BOOKIE_GROUPS, ""));
            String secondaryIsolationGroupString = ConfigurationStringUtil
                    .castToString(properties.getOrDefault(SECONDARY_ISOLATION_BOOKIE_GROUPS, ""));
            if (!primaryIsolationGroupString.isEmpty()) {
                pair.setLeft(Sets.newHashSet(primaryIsolationGroupString.split(",")));
            }
            if (!secondaryIsolationGroupString.isEmpty()) {
                pair.setRight(Sets.newHashSet(secondaryIsolationGroupString.split(",")));
            }
            return pair;
        } else {
            log.info()
                    .attr("policyClass", ensemblePlacementPolicyConfig.getPolicyClass().getName())
                    .log("The ensemble placement policy class is not compatible with "
                            + "IsolatedBookieEnsemblePlacementPolicy, fallback to use defaultIsolationGroups");
            return defaultIsolationGroups;
        }
    }

    @VisibleForTesting
    Set<BookieId> getExcludedBookiesWithIsolationGroups(int ensembleSize,
            Pair<Set<String>, Set<String>> isolationGroups) throws BKNotEnoughBookiesException {
        Set<BookieId> excludedBookies = new HashSet<>();
        if (isolationGroups != null && isWildcard(isolationGroups)) {
            return excludedBookies;
        }
        // Keep isolation counts and exclusions on one cluster view while onClusterChanged updates knownBookies.
        rwLock.readLock().lock();
        try {
            if (snapshot != null) {
                if (ownsSnapshot) {
                    snapshot.refresh();
                }
                var latest = snapshot.latest();
                if (latest == null) {
                    log.warn().onceEvery(Duration.ofMinutes(1))
                            .attr("path", BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH)
                            .log("Refusing isolated bookie placement before the rack configuration is available");
                    throw new BKNotEnoughBookiesException();
                }
                var allGroupsBookieMapping = latest.configuration();
                Set<String> allGroups = allGroupsBookieMapping.keySet();
                if (allGroups.isEmpty()) {
                    return excludedBookies;
                }
                int totalAvailableBookiesInPrimaryGroup = 0;
                Set<String> primaryIsolationGroup = emptySet();
                Set<String> secondaryIsolationGroup = emptySet();
                Set<BookieId> primaryGroupBookies = new HashSet<>();
                if (isolationGroups != null) {
                    primaryIsolationGroup = isolationGroups.getLeft();
                    secondaryIsolationGroup = isolationGroups.getRight();
                }
                for (String group : allGroups) {
                    Set<String> bookiesInGroup = allGroupsBookieMapping.get(group).keySet();
                    if (!primaryIsolationGroup.contains(group)) {
                        for (String bookieAddress : bookiesInGroup) {
                            excludedBookies.add(BookieId.parse(bookieAddress));
                        }
                    } else {
                        for (String groupBookie : bookiesInGroup) {
                            BookieId bookieId = BookieId.parse(groupBookie);
                            if (primaryGroupBookies.add(bookieId)) {
                                totalAvailableBookiesInPrimaryGroup += knownBookies.containsKey(bookieId) ? 1 : 0;
                            }
                        }
                    }
                }

                Set<BookieId> otherGroupBookies = new HashSet<>(excludedBookies);
                Set<BookieId> nonRegionBookies  = new HashSet<>(knownBookies.keySet());
                nonRegionBookies.removeAll(primaryGroupBookies);
                excludedBookies.addAll(nonRegionBookies);

                // sometime while doing isolation, user might not want to remove isolated bookies from other default
                // groups. so, same set of bookies could be overlapped into isolated-group and other default groups. so,
                // try to remove those overlapped bookies from excluded-bookie list because they are also part of
                // isolated-group bookies.
                for (String group : primaryIsolationGroup) {
                    Map<String, BookieInfo> bookieGroup = allGroupsBookieMapping.get(group);
                    if (bookieGroup != null && !bookieGroup.isEmpty()) {
                        for (String bookieAddress : bookieGroup.keySet()) {
                            excludedBookies.remove(BookieId.parse(bookieAddress));
                        }
                    }
                }
                // if primary-isolated-bookies are not enough then add consider secondary isolated bookie group as well.
                int totalAvailableBookiesFromPrimaryAndSecondary = totalAvailableBookiesInPrimaryGroup;
                if (totalAvailableBookiesInPrimaryGroup < ensembleSize) {
                    log.info()
                            .attr("group", primaryIsolationGroup)
                            .attr("group2", secondaryIsolationGroup)
                            .log("Not found enough available-bookies from primary"
                                    + " isolation group [], checking secondary group []");
                    for (String group : secondaryIsolationGroup) {
                        Map<String, BookieInfo> bookieGroup = allGroupsBookieMapping.get(group);
                        if (bookieGroup != null && !bookieGroup.isEmpty()) {
                            for (String bookieAddress : bookieGroup.keySet()) {
                                BookieId bookieId = BookieId.parse(bookieAddress);
                                if (excludedBookies.remove(bookieId) && knownBookies.containsKey(bookieId)) {
                                    totalAvailableBookiesFromPrimaryAndSecondary += 1;
                                }
                            }
                        }
                    }
                }
                if (totalAvailableBookiesFromPrimaryAndSecondary < ensembleSize) {
                    log.info()
                            .attr("group", primaryIsolationGroup)
                            .attr("group2", secondaryIsolationGroup)
                            .log("Not found enough available-bookies from primary"
                                    + " isolation group [] and secondary isolation"
                                    + " group [], checking from non-region bookies");
                    nonRegionBookies.removeAll(otherGroupBookies);
                    for (BookieId bookie: nonRegionBookies) {
                        excludedBookies.remove(bookie);
                    }
                }
            }
        } catch (BKNotEnoughBookiesException e) {
            throw e;
        } catch (Exception e) {
            log.warn().attr("store", e.getMessage()).log("Error getting bookie isolation info from metadata store");
        } finally {
            rwLock.readLock().unlock();
        }
        return excludedBookies;
    }
}
