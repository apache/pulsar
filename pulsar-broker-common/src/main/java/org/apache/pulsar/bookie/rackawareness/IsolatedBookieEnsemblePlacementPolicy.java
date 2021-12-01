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
package org.apache.pulsar.bookie.rackawareness;

import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
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
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class IsolatedBookieEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    public static final String ISOLATION_BOOKIE_GROUPS = "isolationBookieGroups";
    public static final String SECONDARY_ISOLATION_BOOKIE_GROUPS = "secondaryIsolationBookieGroups";

    // Using a pair to save the isolation groups, the left value is the primary group and the right value is
    // the secondary group.
    private ImmutablePair<Set<String>, Set<String>> defaultIsolationGroups;

    private MetadataCache<BookiesRackConfiguration> bookieMappingCache;


    public IsolatedBookieEnsemblePlacementPolicy() {
        super();
    }

    @Override
    public RackawareEnsemblePlacementPolicyImpl initialize(ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer timer, FeatureProvider featureProvider,
            StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {

        Object storeProperty = conf.getProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE);
        if (storeProperty == null) {
            throw new RuntimeException(BookieRackAffinityMapping.METADATA_STORE_INSTANCE
                    + " configuration was not set in the BK client configuration");
        }

        if (!(storeProperty instanceof MetadataStore)) {
            throw new RuntimeException(
                    BookieRackAffinityMapping.METADATA_STORE_INSTANCE + " is not an instance of MetadataStore");
        }

        MetadataStore store = (MetadataStore) storeProperty;

        Set<String> primaryIsolationGroups = new HashSet<>();
        Set<String> secondaryIsolationGroups = new HashSet<>();
        if (conf.getProperty(ISOLATION_BOOKIE_GROUPS) != null) {
            String isolationGroupsString = castToString(conf.getProperty(ISOLATION_BOOKIE_GROUPS));
            if (!isolationGroupsString.isEmpty()) {
                for (String isolationGroup : isolationGroupsString.split(",")) {
                    primaryIsolationGroups.add(isolationGroup);
                }

                // Only add the bookieMappingCache if we have defined an isolation group
                bookieMappingCache = store.getMetadataCache(BookiesRackConfiguration.class);
                bookieMappingCache.get(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH).join();
            }
        }
        if (conf.getProperty(SECONDARY_ISOLATION_BOOKIE_GROUPS) != null) {
            String secondaryIsolationGroupsString = castToString(conf.getProperty(SECONDARY_ISOLATION_BOOKIE_GROUPS));
            if (!secondaryIsolationGroupsString.isEmpty()) {
                for (String isolationGroup : secondaryIsolationGroupsString.split(",")) {
                    secondaryIsolationGroups.add(isolationGroup);
                }
            }
        }
        defaultIsolationGroups = ImmutablePair.of(primaryIsolationGroups, secondaryIsolationGroups);
        return super.initialize(conf, optionalDnsResolver, timer, featureProvider, statsLogger, bookieAddressResolver);
    }

    private static String castToString(Object obj) {
        if (null == obj) {
            return "";
        }
        if (obj instanceof List<?>) {
            List<String> result = new ArrayList<>();
            for (Object o : (List<?>) obj) {
                result.add((String) o);
            }
            return String.join(",", result);
        } else {
            return obj.toString();
        }
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        Map<String, List<String>> isolationGroup = new HashMap<>();
        Set<BookieId> blacklistedBookies = getBlacklistedBookiesWithIsolationGroups(
            ensembleSize, defaultIsolationGroups);
        if (excludeBookies == null) {
            excludeBookies = new HashSet<BookieId>();
        }
        excludeBookies.addAll(blacklistedBookies);
        return super.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        // parse the ensemble placement policy from the custom metadata, if it is present, we will apply it to
        // the isolation groups for filtering the bookies.
        Optional<EnsemblePlacementPolicyConfig> ensemblePlacementPolicyConfig =
            getEnsemblePlacementPolicyConfig(customMetadata);
        Set<BookieId> blacklistedBookies;
        if (ensemblePlacementPolicyConfig.isPresent()) {
            EnsemblePlacementPolicyConfig config = ensemblePlacementPolicyConfig.get();
            Pair<Set<String>, Set<String>> groups = getIsolationGroup(config);
            blacklistedBookies = getBlacklistedBookiesWithIsolationGroups(ensembleSize, groups);
        } else {
            blacklistedBookies = getBlacklistedBookiesWithIsolationGroups(ensembleSize, defaultIsolationGroups);
        }
        if (excludeBookies == null) {
            excludeBookies = new HashSet<BookieId>();
        }
        excludeBookies.addAll(blacklistedBookies);
        return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, currentEnsemble,
                bookieToReplace, excludeBookies);
    }

    private static Optional<EnsemblePlacementPolicyConfig> getEnsemblePlacementPolicyConfig(
        Map<String, byte[]> customMetadata) {

        byte[] ensemblePlacementPolicyConfigData = customMetadata.get(
            EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        if (ensemblePlacementPolicyConfigData != null) {
            try {
                return Optional.ofNullable(EnsemblePlacementPolicyConfig.decode(ensemblePlacementPolicyConfigData));
            } catch (EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException e) {
                log.error("Failed to parse the ensemble placement policy config from the custom metadata", e);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static Pair<Set<String>, Set<String>> getIsolationGroup(EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) {
        MutablePair<Set<String>, Set<String>> pair = new MutablePair<>();
        String className = IsolatedBookieEnsemblePlacementPolicy.class.getName();
        if (ensemblePlacementPolicyConfig.getPolicyClass().getName().equals(className)) {
            Map<String, Object> properties = ensemblePlacementPolicyConfig.getProperties();
            String primaryIsolationGroupString = castToString(properties.getOrDefault(ISOLATION_BOOKIE_GROUPS, ""));
            String secondaryIsolationGroupString = castToString(properties.getOrDefault(SECONDARY_ISOLATION_BOOKIE_GROUPS, ""));
            if (!primaryIsolationGroupString.isEmpty()) {
                pair.setLeft(new HashSet(Arrays.asList(primaryIsolationGroupString.split(","))));
            }
            if (!secondaryIsolationGroupString.isEmpty()) {
                pair.setRight(new HashSet(Arrays.asList(secondaryIsolationGroupString.split(","))));
            }
        }
        return pair;
    }

    private Set<BookieId> getBlacklistedBookiesWithIsolationGroups(int ensembleSize,
        Pair<Set<String>, Set<String>> isolationGroups) {
        Set<BookieId> blacklistedBookies = new HashSet<>();
        try {
            if (bookieMappingCache != null) {
                CompletableFuture<Optional<BookiesRackConfiguration>> future =
                        bookieMappingCache.get(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH);

                Optional<BookiesRackConfiguration> optRes = (future.isDone() && !future.isCompletedExceptionally())
                        ? future.join() : Optional.empty();

                if (!optRes.isPresent()) {
                    return blacklistedBookies;
                }

                BookiesRackConfiguration allGroupsBookieMapping = optRes.get();
                Set<String> allBookies = allGroupsBookieMapping.keySet();
                int totalAvailableBookiesInPrimaryGroup = 0;
                Set<String> primaryIsolationGroup = Collections.emptySet();
                Set<String> secondaryIsolationGroup = Collections.emptySet();
                if (isolationGroups != null) {
                    primaryIsolationGroup = isolationGroups.getLeft();
                    secondaryIsolationGroup = isolationGroups.getRight();
                }
                for (String group : allBookies) {
                    Set<String> bookiesInGroup = allGroupsBookieMapping.get(group).keySet();
                    if (!primaryIsolationGroup.contains(group)) {
                        for (String bookieAddress : bookiesInGroup) {
                            blacklistedBookies.add(BookieId.parse(bookieAddress));
                        }
                    } else {
                        for (String groupBookie : bookiesInGroup) {
                            totalAvailableBookiesInPrimaryGroup += knownBookies
                                .containsKey(BookieId.parse(groupBookie)) ? 1 : 0;
                        }
                    }
                }
                // sometime while doing isolation, user might not want to remove isolated bookies from other default
                // groups. so, same set of bookies could be overlapped into isolated-group and other default groups. so,
                // try to remove those overlapped bookies from excluded-bookie list because they are also part of
                // isolated-group bookies.
                for (String group : primaryIsolationGroup) {
                    Map<String, BookieInfo> bookieGroup = allGroupsBookieMapping.get(group);
                    if (bookieGroup != null && !bookieGroup.isEmpty()) {
                        for (String bookieAddress : bookieGroup.keySet()) {
                            blacklistedBookies.remove(BookieId.parse(bookieAddress));
                        }
                    }
                }
                // if primary-isolated-bookies are not enough then add consider secondary isolated bookie group as well.
                if (totalAvailableBookiesInPrimaryGroup < ensembleSize) {
                    log.info(
                        "Not found enough available-bookies from primary isolation group [{}] , checking secondary group [{}]",
                        primaryIsolationGroup, secondaryIsolationGroup);
                    for (String group : secondaryIsolationGroup) {
                        Map<String, BookieInfo> bookieGroup = allGroupsBookieMapping.get(group);
                        if (bookieGroup != null && !bookieGroup.isEmpty()) {
                            for (String bookieAddress : bookieGroup.keySet()) {
                                blacklistedBookies.remove(BookieId.parse(bookieAddress));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error getting bookie isolation info from metadata store: {}", e.getMessage());
        }
        return blacklistedBookies;
    }
}
