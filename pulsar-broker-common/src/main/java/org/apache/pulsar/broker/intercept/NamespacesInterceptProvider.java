package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

import java.util.List;
import java.util.Set;

public interface NamespacesInterceptProvider {
    /**
     * Intercept call for creating namespace
     *
     * @param namespaceName the namespace name
     * @param policies polices for this namespace
     * @param clientRole the role used to create namespace
     */
    default void createNamespace(NamespaceName namespaceName, Policies policies, String clientRole) throws InterceptException {}

    default void deleteNamespace(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void deleteNamespaceBundle(NamespaceName namespaceName, String bundleRange, String clientRole) throws InterceptException {}

    default void setNamespaceMessageTTL(NamespaceName namespaceName, int messageTTL, String clientRole) throws InterceptException {}

    default void grantPermissionOnNamespace(NamespaceName namespaceName, String role, Set<AuthAction> actions, String clientRole) throws InterceptException {}

    default void grantPermissionOnSubscription(NamespaceName namespaceName, String subscription, Set<String> roles, String clientRole) throws InterceptException {}

    default void revokePermissionsOnNamespace(NamespaceName namespaceName, String role, String clientRole) throws InterceptException {}

    default void revokePermissionsOnSubscription(NamespaceName namespaceName, String subscriptionName, String role, String clientRole) throws InterceptException {}

    default void getNamespaceReplicationClusters(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setNamespaceReplicationClusters(NamespaceName namespaceName, List<String> clusterIds, String clientRole) throws InterceptException {}

    default void modifyDeduplication(NamespaceName namespaceName, boolean enableDeduplication, String clientRole) {}

    default void getTenantNamespaces(NamespaceName namespaceName, String tenant, String clientRole) throws InterceptException {}

    default void unloadNamespace(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setBookieAffinityGroup(NamespaceName namespaceName, BookieAffinityGroupData bookieAffinityGroup, String clientRole) throws InterceptException {}

    default void getBookieAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void unloadNamespaceBundle(NamespaceName namespaceName, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {}

    default void splitNamespaceBundle(NamespaceName namespaceName, String bundleRange, boolean authoritative, boolean unload, String clientRole) throws InterceptException {}

    default void setTopicDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {}

    default void getTopicDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException  {}

    default void setSubscriptionDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {}

    default void getSubscriptionDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setSubscribeRate(NamespaceName namespaceName, SubscribeRate subscribeRate, String clientRole) throws InterceptException {}

    default void getSubscribeRate(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setReplicatorDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {}

    default void getReplicatorDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setBacklogQuota(NamespaceName namespaceName, BacklogQuota.BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota, String clientRole) throws InterceptException {}

    default void removeBacklogQuota(NamespaceName namespaceName, BacklogQuota.BacklogQuotaType backlogQuotaType, String clientRole) throws InterceptException {}

    default void setRetention(NamespaceName namespaceName, RetentionPolicies retention, String clientRole) throws InterceptException {}

    default void setPersistence(NamespaceName namespaceName, PersistencePolicies persistence, String clientRole) throws InterceptException {}

    default void getPersistence(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void clearNamespaceBacklog(NamespaceName namespaceName, boolean authoritative, String clientRole) throws InterceptException {}

    default void clearNamespaceBundleBacklog(NamespaceName namespaceName, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {}

    default void clearNamespaceBacklogForSubscription(NamespaceName namespaceName, String subscription, boolean authoritative, String clientRole) throws InterceptException {}

    default void clearNamespaceBundleBacklogForSubscription(NamespaceName namespaceName, String subscription, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {}

    default void unsubscribeNamespace(NamespaceName namespaceName, String subscription, boolean authoritative, String clientRole) throws InterceptException {}

    default void unsubscribeNamespaceBundle(NamespaceName namespaceName, String subscription, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {}

    default void setSubscriptionAuthMode(NamespaceName namespaceName, SubscriptionAuthMode subscriptionAuthMode, String clientRole) throws InterceptException {}

    default void modifyEncryptionRequired(NamespaceName namespaceName, boolean encryptionRequired, String clientRole) throws InterceptException {}

    default void setNamespaceAntiAffinityGroup(NamespaceName namespaceName, String antiAffinityGroup, String clientRole) throws InterceptException {}

    default void getNamespaceAntiAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void removeNamespaceAntiAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void getAntiAffinityNamespaces(NamespaceName namespaceName, String cluster, String antiAffinityGroup, String clientRole) throws InterceptException {}

    default void getMaxProducersPerTopic(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setMaxProducersPerTopic(NamespaceName namespaceName, int maxProducersPerTopic, String clientRole) throws InterceptException {}

    default void getMaxConsumersPerTopic(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setMaxConsumersPerTopic(NamespaceName namespaceName, int maxConsumersPerTopic, String clientRole) throws InterceptException {}

    default void getMaxConsumersPerSubscription(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setMaxConsumersPerSubscription(NamespaceName namespaceName, int maxConsumersPerSubscription, String clientRole) throws InterceptException {}

    default void getCompactionThreshold(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setCompactionThreshold(NamespaceName namespaceName, long newThreshold, String clientRole) throws InterceptException {}

    default void getOffloadThreshold(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setOffloadThreshold(NamespaceName namespaceName, long newThreshold, String clientRole) throws InterceptException {}

    default void getOffloadDeletionLag(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setOffloadDeletionLag(NamespaceName namespaceName, Long newDeletionLagMs, String clientRole) throws InterceptException {}

    default void getSchemaAutoUpdateCompatibilityStrategy(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setSchemaAutoUpdateCompatibilityStrategy(NamespaceName namespaceName,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy, String clientRole) throws InterceptException {}

    default void getSchemaValidationEnforced(NamespaceName namespaceName, String clientRole) throws InterceptException {}

    default void setSchemaValidationEnforced(NamespaceName namespaceName, boolean schemaValidationEnforced, String clientRole) throws InterceptException {}
}
