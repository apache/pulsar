package org.apache.pulsar.common.policies.data;

public enum ClusterOperation {
    LIST_CLUSTERS,
    GET_CLUSTER,
    CREATE_CLUSTER,
    UPDATE_CLUSTER,
    DELETE_CLUSTER,

    // detailed update
    GET_PEER_CLUSTER,
    UPDATE_PEER_CLUSTER,
    GET_FAILURE_DOMAIN,
    UPDATE_FAILURE_DOMAIN,
    DELETE_FAILURE_DOMAIN
}
