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

public class NamespacesInterceptService {

    private final NamespacesInterceptProvider provider;

    public NamespacesInterceptService(NamespacesInterceptProvider namespaceInterceptProvider) {
        this.provider = namespaceInterceptProvider;
    }

    /**
     * Intercept call for creating namespace
     *
     * @param namespaceName the namespace name
     * @param policies polices for this namespace
     * @param clientRole the role used to create namespace
     */
    public void createNamespace(NamespaceName namespaceName, Policies policies, String clientRole) throws InterceptException {
        provider.createNamespace(namespaceName, policies, clientRole);
    }

    public void deleteNamespace(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.deleteNamespace(namespaceName, clientRole);
    }

    public void deleteNamespaceBundle(NamespaceName namespaceName, String bundleRange, String clientRole) throws InterceptException {
        provider.deleteNamespaceBundle(namespaceName, bundleRange, clientRole);
    }

    public void setNamespaceMessageTTL(NamespaceName namespaceName, int messageTTL, String clientRole) throws InterceptException {
        provider.setNamespaceMessageTTL(namespaceName, messageTTL, clientRole);
    }

    public void grantPermissionOnNamespace(NamespaceName namespaceName, String role, Set<AuthAction> actions, String clientRole) throws InterceptException {
        provider.grantPermissionOnNamespace(namespaceName, role, actions, clientRole);
    }

    public void grantPermissionOnSubscription(NamespaceName namespaceName, String subscription, Set<String> roles, String clientRole) throws InterceptException {
        provider.grantPermissionOnSubscription(namespaceName, subscription, roles, clientRole);
    }

    public void revokePermissionsOnNamespace(NamespaceName namespaceName, String role, String clientRole) throws InterceptException {
        provider.revokePermissionsOnNamespace(namespaceName, role, clientRole);
    }

    public void revokePermissionsOnSubscription(NamespaceName namespaceName, String subscriptionName, String role, String clientRole) throws InterceptException {
        provider.revokePermissionsOnSubscription(namespaceName, subscriptionName, role, clientRole);
    }

    public void getNamespaceReplicationClusters(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getNamespaceReplicationClusters(namespaceName, clientRole);
    }

    public void setNamespaceReplicationClusters(NamespaceName namespaceName, List<String> clusterIds, String clientRole) throws InterceptException {
        provider.setNamespaceReplicationClusters(namespaceName, clusterIds, clientRole);
    }

    public void modifyDeduplication(NamespaceName namespaceName, boolean enableDeduplication, String clientRole) throws InterceptException {
        provider.modifyDeduplication(namespaceName, enableDeduplication, clientRole);
    }

    public void getTenantNamespaces(NamespaceName namespaceName, String tenant, String clientRole) throws InterceptException {
        provider.getTenantNamespaces(namespaceName, tenant, clientRole);
    }

    public void unloadNamespace(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.unloadNamespace(namespaceName, clientRole);
    }

    public void setBookieAffinityGroup(NamespaceName namespaceName, BookieAffinityGroupData bookieAffinityGroup, String clientRole) throws InterceptException {
        provider.setBookieAffinityGroup(namespaceName, bookieAffinityGroup, clientRole);
    }

    public void getBookieAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getBookieAffinityGroup(namespaceName, clientRole);
    }

    public void unloadNamespaceBundle(NamespaceName namespaceName, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {
        provider.unloadNamespaceBundle(namespaceName, bundleRange, authoritative, clientRole);
    }

    public void splitNamespaceBundle(NamespaceName namespaceName, String bundleRange, boolean authoritative, boolean unload, String clientRole) throws InterceptException {
        provider.splitNamespaceBundle(namespaceName, bundleRange, authoritative, unload, clientRole);
    }

    public void setTopicDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {
        provider.setTopicDispatchRate(namespaceName, dispatchRate, clientRole);
    }

    public void getTopicDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException  {
        provider.getTopicDispatchRate(namespaceName, clientRole);
    }

    public void setSubscriptionDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {
        provider.setSubscriptionDispatchRate(namespaceName, dispatchRate, clientRole);
    }

    public void getSubscriptionDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getSubscriptionDispatchRate(namespaceName, clientRole);
    }

    public void setSubscribeRate(NamespaceName namespaceName, SubscribeRate subscribeRate, String clientRole) throws InterceptException {
        provider.setSubscribeRate(namespaceName, subscribeRate, clientRole);
    }

    public void getSubscribeRate(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getSubscribeRate(namespaceName, clientRole);
    }

    public void setReplicatorDispatchRate(NamespaceName namespaceName, DispatchRate dispatchRate, String clientRole) throws InterceptException {
        provider.setReplicatorDispatchRate(namespaceName, dispatchRate, clientRole);
    }

    public void getReplicatorDispatchRate(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getReplicatorDispatchRate(namespaceName, clientRole);
    }

    public void setBacklogQuota(NamespaceName namespaceName, BacklogQuota.BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota, String clientRole) throws InterceptException {
        provider.setBacklogQuota(namespaceName, backlogQuotaType, backlogQuota, clientRole);
    }

    public void removeBacklogQuota(NamespaceName namespaceName, BacklogQuota.BacklogQuotaType backlogQuotaType, String clientRole) throws InterceptException {
        provider.removeBacklogQuota(namespaceName, backlogQuotaType, clientRole);
    }

    public void setRetention(NamespaceName namespaceName, RetentionPolicies retention, String clientRole) throws InterceptException {
        provider.setRetention(namespaceName, retention, clientRole);
    }

    public void setPersistence(NamespaceName namespaceName, PersistencePolicies persistence, String clientRole) throws InterceptException {
        provider.setPersistence(namespaceName, persistence, clientRole);
    }

    public void getPersistence(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getPersistence(namespaceName, clientRole);
    }

    public void clearNamespaceBacklog(NamespaceName namespaceName, boolean authoritative, String clientRole) throws InterceptException {
        provider.clearNamespaceBacklog(namespaceName, authoritative, clientRole);
    }

    public void clearNamespaceBundleBacklog(NamespaceName namespaceName, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {
        provider.clearNamespaceBundleBacklog(namespaceName, bundleRange, authoritative, clientRole);
    }

    public void clearNamespaceBacklogForSubscription(NamespaceName namespaceName, String subscription, boolean authoritative, String clientRole) throws InterceptException {
        provider.clearNamespaceBacklogForSubscription(namespaceName, subscription, authoritative, clientRole);
    }

    public void clearNamespaceBundleBacklogForSubscription(NamespaceName namespaceName, String subscription, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {
        provider.clearNamespaceBundleBacklogForSubscription(namespaceName, subscription, bundleRange, authoritative, clientRole);
    }

    public void unsubscribeNamespace(NamespaceName namespaceName, String subscription, boolean authoritative, String clientRole) throws InterceptException {
        provider.unsubscribeNamespace(namespaceName, subscription, authoritative, clientRole);
    }

    public void unsubscribeNamespaceBundle(NamespaceName namespaceName, String subscription, String bundleRange, boolean authoritative, String clientRole) throws InterceptException {
        provider.unsubscribeNamespaceBundle(namespaceName, subscription, bundleRange, authoritative, clientRole);
    }

    public void setSubscriptionAuthMode(NamespaceName namespaceName, SubscriptionAuthMode subscriptionAuthMode, String clientRole) throws InterceptException {
        provider.setSubscriptionAuthMode(namespaceName, subscriptionAuthMode, clientRole);
    }

    public void modifyEncryptionRequired(NamespaceName namespaceName, boolean encryptionRequired, String clientRole) throws InterceptException {
        provider.modifyEncryptionRequired(namespaceName, encryptionRequired, clientRole);
    }

    public void setNamespaceAntiAffinityGroup(NamespaceName namespaceName, String antiAffinityGroup, String clientRole) throws InterceptException {
        provider.setNamespaceAntiAffinityGroup(namespaceName, antiAffinityGroup, clientRole);
    }

    public void getNamespaceAntiAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getNamespaceAntiAffinityGroup(namespaceName, clientRole);
    }

    public void removeNamespaceAntiAffinityGroup(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.removeNamespaceAntiAffinityGroup(namespaceName, clientRole);
    }

    public void getAntiAffinityNamespaces(NamespaceName namespaceName, String cluster, String antiAffinityGroup, String clientRole) throws InterceptException {
        provider.getAntiAffinityNamespaces(namespaceName, cluster, antiAffinityGroup, clientRole);
    }

    public void getMaxProducersPerTopic(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getMaxProducersPerTopic(namespaceName, clientRole);
    }

    public void setMaxProducersPerTopic(NamespaceName namespaceName, int maxProducersPerTopic, String clientRole) throws InterceptException {
        provider.setMaxProducersPerTopic(namespaceName, maxProducersPerTopic, clientRole);
    }

    public void getMaxConsumersPerTopic(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getMaxConsumersPerTopic(namespaceName, clientRole);
    }

    public void setMaxConsumersPerTopic(NamespaceName namespaceName, int maxConsumersPerTopic, String clientRole) throws InterceptException {
        provider.setMaxConsumersPerTopic(namespaceName, maxConsumersPerTopic, clientRole);
    }

    public void getMaxConsumersPerSubscription(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getMaxConsumersPerSubscription(namespaceName, clientRole);
    }

    public void setMaxConsumersPerSubscription(NamespaceName namespaceName, int maxConsumersPerSubscription, String clientRole) throws InterceptException {
        provider.setMaxConsumersPerSubscription(namespaceName, maxConsumersPerSubscription, clientRole);
    }

    public void getCompactionThreshold(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getCompactionThreshold(namespaceName, clientRole);
    }

    public void setCompactionThreshold(NamespaceName namespaceName, long newThreshold, String clientRole) throws InterceptException {
        provider.setCompactionThreshold(namespaceName, newThreshold, clientRole);
    }

    public void getOffloadThreshold(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getOffloadThreshold(namespaceName, clientRole);
    }

    public void setOffloadThreshold(NamespaceName namespaceName, long newThreshold, String clientRole) throws InterceptException {
        provider.setOffloadThreshold(namespaceName, newThreshold, clientRole);
    }

    public void getOffloadDeletionLag(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getOffloadDeletionLag(namespaceName, clientRole);
    }

    public void setOffloadDeletionLag(NamespaceName namespaceName, Long newDeletionLagMs, String clientRole) throws InterceptException {
        provider.setOffloadDeletionLag(namespaceName, newDeletionLagMs, clientRole);
    }

    public void getSchemaAutoUpdateCompatibilityStrategy(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getSchemaAutoUpdateCompatibilityStrategy(namespaceName, clientRole);
    }

    public void setSchemaAutoUpdateCompatibilityStrategy(NamespaceName namespaceName,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy, String clientRole) throws InterceptException {
        provider.setSchemaAutoUpdateCompatibilityStrategy(namespaceName, strategy, clientRole);
    }

    public void getSchemaValidationEnforced(NamespaceName namespaceName, String clientRole) throws InterceptException {
        provider.getSchemaValidationEnforced(namespaceName, clientRole);
    }

    public void setSchemaValidationEnforced(NamespaceName namespaceName, boolean schemaValidationEnforced, String clientRole) throws InterceptException {
        provider.setSchemaValidationEnforced(namespaceName, schemaValidationEnforced, clientRole);
    }
}
