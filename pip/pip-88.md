# PIP-88: Replicate schemas across multiple

- Status: Proposal
- Authors: Lin Lin, Hang Chen, Penghui Li
- Pull Request: https://github.com/apache/pulsar/pull/11441
- Mailing List discussion: https://lists.apache.org/thread.html/rd35af2122782d374ff819b9b14386851742622661107fa6229973851%40%3Cdev.pulsar.apache.org%3E
- Release:

### Background

Currently, if enabled data replication between different clusters and the data is written with schema, the data will not available for reading at the remote cluster because the schemas of the topic are not replicated to the remote cluster, the consumer or reader will encounter schema not found exception when reading data from the topic.

So the purpose of this proposal is to support reading messages with schema from the remote cluster. Before starting to discuss how to achieve the purpose, I will discuss the schema version first in Pulsar, because this will be an important concept repeatedly mentioned in subsequent discussions.

In Pulsar, the schema version is incremental within a cluster, the newly added schema will be stored into the schema registry with a schema version(last schema version + 1). When the producer publishes messages to a topic, the producer will try to register the schema to the schema registry and will get a responded schema version, of course, if the schema already exists, the schema will not put into the schema registry again. And then the responded schema version will be used in the message metadata. For a consumer, it will fetch the schema by the schema version that the message used for the message deserialization.

For sharing the schema between different clusters, we can have different ways

- A centralized schema registry that can be accessed by each cluster
- Fetch the schema from the remote cluster directly
- Replicate the schema to the remote clusters

Both of them can share the schema between different clusters, but considering the deployment complexity and maintenance, and each cluster should have the ability to run independently. So the last one is the proposed solution for achieving schema sharing.

For the first approach, a centralized schema registry will be a required service for each cross-region cluster. This will bring the complexity of the schema registry and maintenance. And the centralized schema registry means a topic across multiple clusters must have the same schema sequence. But in some cases, the topic might have different schema sequences for different clusters such as replicate data from the edge Pulsar service to the data center. Keep the schema system independent of clusters is also more in line with the principle that each cluster can be run separately.
For the second approach, we don’t need a centralized schema registry and replicate schemas across different clusters, but It will violate the principle that the cluster can be run separately because reading data from a topic for a cluster will depend on the schema that in another cluster. And this will make the schema compatibility check more complex because a cluster might do not have all schemas of the existing data for a topic. It needs to fetch all schemas from the remote cluster and correct the order in which they were added.

So, let us discuss the details of the third approach in the next section.

### Solution

In this section, we will focus on the third approach introduced in the previous section. So the key point we will discuss later is how to replicate schema to the remote cluster and how to deal with the impact of this on the current schema compatibility check.

The replicator using a durable cursor for reading the data(entry/batch) from the ManagedLedger and then using a producer to publish data to the remote cluster. This solution is very similar to the AUTO_PRODUCE_BYTES schema introduced by PIP 43 https://github.com/apache/pulsar/wiki/PIP-43%3A-producer-send-message-with-different-schema. If the message with a new schema has not registered to the remote cluster, the producer will create the schema to the remote cluster first and then use the schema version of the newly added schema to publish messages to the remote cluster.

So, the schema version of the message that will replicate to the remote cluster will be rewritten during the replication process. Since we already deserialize the message metadata for setting the replicate_from field, so rewrite the schema version will not bring more resource consumption. So the same message in different clusters might have different schema versions but the same schema, of course, this doesn’t happen normally.

If there are some messages that have not been replicated to the remote clusters and the messages are not with the latest schema, at this time a new schema is added to the remote cluster. Or the messages are replicated from multiple clusters with different schemas, this will also lead to the inconsistent schema version across clusters.

For the schema evolution, users might consider the data replication state according to the different schema compatibility check strategies of the topic when they add a new schema to the topic. Just make sure the new schema will not prevent the data replication from the remote cluster. A simple way is to check if all the schemas from the remote clusters are replicated to the local cluster before adding a new schema to the local cluster for a topic, of course, if the schema compatibility check strategy for the topic is ALWAYS_COMPATIBLE, looks like pre-check is redundant.

If the replicator encounters the schema incompatibility issue during the replication, it will stop the data replication until the problem is resolved. Or users want to skip the incompatible data, just need to reset the cursor for the replicator, but this is dangerous because this will lost the data from the remote cluster.

### Implementation

Just follow the PIP 43 implementation to enhance the replicator. And most of the current components can be reused such as schema cache at the client-side.
Compatibility
The approach will not introduce any compatibility issues.
### Tests Plan
- Make sure the message with schema can be consumed at the remote cluster
- The topic policy using the Avro schema, so the topic policy should available for the remote cluster
- Make sure the replicator will retry to replicate the schema if got an exception from the remote cluster except for the incompatible exception
