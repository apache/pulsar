# PIP-8: Pulsar beyond 1M topics

* **Status**: Implemented
 * **Pull Request**: [#903](https://github.com/apache/incubator-pulsar/pull/903)

# Introduction
Pulsar was designed to serve 1M topics in a cluster. We have a need to scale Pulsar beyond 1M topics.
Recent improvements have stretched this 1M limit somewhat, but Pulsar startup time  from a Cloud state with 1M+ topics starts to increase with number of topics.  This proposal is to scale beyond 1M+ topics, and still keep a a limit on startup time

## Functional requirements

* Must not require a client upgrade (Pulsar wire protocol doesn’t change)
* Must be backward compatible
* Must provide a painless upgrade

## Proposal
This document describes a high level design for extending Pulsar “namespace” space (hereafter termed NS space) across more than one Pulsar cluster. The Global NS space of a Pulsar cluster is disambiguated using ZK paths in the global ZK. Topics are not stored in Global ZK. Pulsar  can support tens of thousands of  NS paths, and the Global ZK can scale to a that many thousands of namespaces. If each NS can support 1-1000 topics, Pulsar can reach 10M topics. 

If the NS space can be partitioned across multiple Pulsar clusters (effectively more local ZKs) , then the clusters can share the topic load. With the abstraction of a single NS space across multiple Pulsar clusters, Pulsar then scales to multiple millions of topics.  

## Current Pulsar architecture
Each region has one Pulsar cluster. One Global ZK, one local ZK, along with the brokers and bookies form a Pulsar cluster.
<img width="737" alt="currentpulsar" src="https://user-images.githubusercontent.com/22042978/32640080-b87e8898-c57b-11e7-8b8e-4aad5c397b6b.png">
## Proposed Pulsar architecture
The new architecture will have more than one Pulsar cluster in each region. The figure below is an example with Pulsar across 2 regions, with 2 local clusters in each.
<img width="736" alt="newpulsar" src="https://user-images.githubusercontent.com/22042978/32640079-b5edd41c-c57b-11e7-9021-3543288cd4c0.png">
## Scope of changes
The Global NS space is unique for global namespaces, so this does not require any special consideration. The only problem to be resolved is how the NS space is to be distributed among a particular group of local clusters. The NS space assignment needs to deterministic and stateful within a cluster. If that can be achieved, all existing mechanisms of administration and operation would continue to work. The local clusters in effect become distinct clusters with no interaction, except than using the Global ZK to ensure there are no collisions in the NS space across them. Fortunately, there is no need to make any changes to Pulsar to make the NS assignment stateful and deterministic. 
## Peer groups
We here introduce the concept of a peer group (peers). This is the group of clusters that together hosts an instance of the Global NS space. 

Peer groups are defined and set in the Global ZK. Each local cluster has its own defined peer group, which is set administratively.

## Creation of Namespaces
Namespaces are created as usual, and results in an update to the Global ZK. The namespace node gets created in the Global ZK. Replication clusters are also set as usual, for eg, C3 and C4 for NS2. This is the local clusters where the namespace resides, and the topics in that namespace are created in the specific local ZK for those clusters. So far, this is nothing different from existing functionality.

A Namespace resides on specific local clusters, as identified by its replication set, Provisioning tenants  and Namespaces will still need to be done on specific local clusters
## Lookup
To maintain the abstraction of a single cluster, a local cluster needs to be smart enough to know the peers which share the Global NS space with it. When a local cluster receives a lookup request, the local cluster looks up the namespace, to see if it is present in the Global ZK. The receiving cluster then sees if it is the owner of that namespace. If it is, then the lookup is completed in the local cluster. If not, it then redirects it to the next local cluster in the peer group, (and so on…), with the appropriate hints to break lookup cycles.

With the concept of a peer group and the implementation of cluster redirect in lookups, the NS space can be spread across more than one local Pulsar cluster
Lookup URLs
The peer group as a whole needs to get a new URL, which fronts the entire list of brokers of the peer group. The local clusters will need local lookup URLs which are needed for inter-peer group cluster redirects.

## Caveats and other considerations
Replication within a peer group will be disallowed, as it violates the namespace uniqueness requirement. Attempting to do so will fail automatically if the local clusters share the same Global ZK installation. Otherwise it will have to be enforced at the API implementation level. The examples above show a Global ZK installation for each local cluster, but that is not necessary.

Nothing in this design requires that we have an identical regions of Pulsar clusters across all datacenters. For eg: We could have one local cluster in one datacentre, and 2 in another.
<img width="741" alt="anypulsar" src="https://user-images.githubusercontent.com/22042978/32640020-6e3e6f6e-c57b-11e7-871a-e5e8ca85d92b.png">

## Upgrade 

This is the scenario where Pulsar is running in a single local cluster mode, and needs to add a new local cluster to it. 

We will prefer that replication URLs be the local cluster URLs. So the existing URLs will need to remain as local cluster URLs. 

This means that we will need to create a new peer group URL.

Existing clients will then need to update their URL settings to the peer group URL. During the upgrade period, both URLs, - the new peer group URL and the existing local cluster URL - will front end the same group of brokers; the existing cluster brokers. After the clients change the setting to the peer group URL, the peer group URL can front-end both set of brokers, from the existing local cluster and the new cluster.

Strictly speaking, this client setting change is not required; the only drawback to not doing this is that all lookups will always end up in the existing local cluster. 

## How to setup peer-clusters
If each region has multiple pulsar clusters setup then we can configure peer-clusters that exist into same region.

eg:

**1. Setup Pulsar clusters in each region:**

We have two regions `us-west` and `us-east`, and each region has 3 pulsar clusters setup.
Pulsar clusters setup in `us-west` region: `us-west-1`, `us-west-2`, `us-west-3`
```
bin/pulsar-admin clusters create us-west-1 --url http://pulsar.us-west-1.com
bin/pulsar-admin clusters create us-west-2 --url http://pulsar.us-west-2.com
bin/pulsar-admin clusters create us-west-3 --url http://pulsar.us-west-3.com
```

Pulsar clusters setup in `us-east` region: `us-east-1`, `us-east-2`, `us-east-3`
```
bin/pulsar-admin clusters create us-east-1 --url http://pulsar.us-east-1.com
bin/pulsar-admin clusters create us-east-2 --url http://pulsar.us-east-2.com
bin/pulsar-admin clusters create us-east-3 --url http://pulsar.us-east-3.com
```

**2. Configure peer-clusters which have been setup in the same region**

a.  Setup `us-west-2` and `us-west-3` as peer-clusters of `us-west-1`. So, if `us-west-1` receives lookup request for namespace that would be owned by any peer-cluster then `us-west-1` can redirect that lookup request to appropriate cluster.

`bin/pulsar-admin clusters update-peer-clusters us-west-1 --peer-clusters us-west-2,us-west-3`

b. Setup `us-east-2` and `us-east-3` as peer-clusters of `us-east-1`. 

`bin/pulsar-admin clusters update-peer-clusters us-east-1 --peer-clusters us-east-2,us-east-3`

**3. Create namespace with geo-replication enabled**

`bin/pulsar-admin namespaces create sample/global-ns -c us-west-3,us-east-3`

Now, if `us-west-1` receives lookup request for namespace `sample/global-ns` then `us-west-1` knows about its peer-cluster `us-west-3` and it redirects lookup to `us-west-3` so, `us-west-3` can serve all the topics under that namespace. Same applies to `us-east` peer-clusters as well.
