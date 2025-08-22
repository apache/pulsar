# PIP-92: Topic policy across multiple clusters

- Status: Proposal
- Authors: Penghui Li、Chen Hang、 Lin Lin
- Pull Request:
- Mailing List discussion:
- Release:
# Motivation
When setting the topic policy for a geo-replicated cluster, some policies want to affect the whole geo-replicated cluster but some only want to affect the local cluster. So the proposal is to support global topic policy and local topic policy.
# Approach
Currently, we are using the TopicPolicies construction to store the topic policy for a topic. An easy way to achieve different topic policies for multiple clusters is to add a global flag to TopicPolicies .Replicator will replicate policies with a global flag to other clusters 

```
public class TopicPolicies {
     bBoolean isGlobal = false
}
```
We only cache one local Topic Policies in the memory before. Now it will become two, one is Local and the other is Global.

After adding the global topic policy, the topic applied priority is:

Local cluster topic policy
Global topic policy
Namespace policy
Broker default configuration

When setting a global topic policy, we can use the `--global` option, it should be:

```
bin/pulsar-admin topics set-retention -s 1G -t 1d --global my-topic
```
If the --global option is not added, the behavior is consistent with before, and only updates the local policies.
Topic policies are stored in System Topic, we can directly use Replicator to replicate data to other Clusters. We need to add a new API to the Replicator interface, which can set the Filter Function. Then add a Function to filter out the data with isGlobal = false

Delete a cluster?
Deleting a cluster will now delete local topic policies and will not affect other clusters, because only global policies will be replicated to other clusters
Changes
Every topic policy API adds the `--global` option. Including broker REST API, Admin SDK, CMD.

Add API `public void setFilterFunction(Function<Message, Boolean>)`. If Function returns false, then filter out the input message.

# Compatibility
The solution does not introduce any compatibility issues, `isGlobal` in TopicPolicies is false by default in existing Policies.

# Test Plan
Existing TopicPolicies will not be affected
Only replicate Global Topic Policies, FilterFunction can run as expected
Priority of Topic Policies matches our setting
