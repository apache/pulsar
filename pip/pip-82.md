# PIP-82: Tenant and namespace level rate limiting

* **Status**: Proposal
* **Authors**: Bharani Chadalavada, Kaushik Ghosh, Ravi Vaidyanathan, Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Motivation

Currently in Pulsar, it is possible to configure rate limiting, in
terms of messages/sec or bytes/sec both on the producers or the
consumers for a topic. The rates are configured in the namespace
policies and the enforcement is done at the topic level, or at the
partition level, in the case of a partitioned topic.

The fact that rate is enforced at topic level doesn’t allow to control
the max rate across a given namespace (a namespace can span multiple
brokers). For example if the limit is 100msg/s per topic, a user can
simply create more topics to keep increasing the load on the system.

Instead, we should have a way to better define producers and consumers
limit for a namespace or a Pulsar tenant and have the Pulsar brokers
to collectively enforce them.

## Goal

The goal for this feature is to allow users to configure a namespace
or tenant wide limit for producers and consumers and have that
enforced irrespective of the number of topics in the namespace, with
fair sharing of the quotas.

Another important aspect is that the quota enforcement needs to be
able to dynamically adjust when the quota is raised or reduced.

### Non-goals

It is not a goal to provide a super strict limiter, rather the
implementation would be allowed to either undercount or overcount for
short amounts of time, as long as the limiting converges close to the
configured quota, with an approximation of, say, 10%.

It is not a goal to allow users to configure limits at multiple levels
(tenant/namespace/topic) and implement a hierarchical enforcement
mechanism.

If the limits are configured at tenant level, it is not a goal to
evenly distribute the quotas across all namespaces. Similarly if the
limits are configured at namespace level, it is not a goal to evenly
distribute the quota across all topics in the namespace.

Rate Limit configured via the ResourceGroup will not be enforced on
cross-cluster replication.

## Implementation

### Configuration of quotas

In order to implement the rate limiting per namespace or tenant, we’re
going to introduce the concept of a “ResourceGroup”. A ResourceGroup
is defined as the grouping of different rate limit quotas and it can
be associated with different resources, for example a Pulsar tenant or
a Pulsar namespace to start with. The usage quota on the ResourceGroup
is shared among all the entities (tenants, namespaces) that are attached
to the resource group. 


In addition to rate limiting (in bytes/s and msg/s), for producers and
consumers, the configuration of the ResourceGroup might also contain
other quotas in the future [ for e.g., storage quota].

### Enforcement

In order to enforce the limit over several topics that belong to a
particular namespace, we need to have multiple brokers to cooperate
with each other with a feedback mechanism. With this each broker will
be able to know, within the scope of a particular ResourceGroup, how
much of the portion of the quota is currently being used by other
brokers.

Each broker will then make sure that the available quota is split
optimally between the brokers who are requesting it.

Note: Pulsar currently supports topic/partition level rate-limiting,
if that is configured along with the new namespace wide rate-limiting
using resource groups then both configurations will be effective. In
effect, at the broker level the old config will be enforced and also
the namespace level rate-limiter will be enforced, so the more
stringent of the two will get enforced.

At some point in the future it will be good to make topic/partition
quota configuration to fit within the namespace level ratelimiter and
more self-explanatory. At that point the old configuration could be
deprecated over time, Not in the scope of this feature though.


#### Communications between brokers

Brokers will be talking to each other using a regular Pulsar
topic. For the purposes of this feature, a non-persistent topic will
be the ideal choice to have minimum resources requirement and always
giving the last data value. We can mostly ignore the data losses as
part of an “undercounting” event which will lead to exceed the quota
for a brief amount of time.

Each broker will publish the current actual usage, as an absolute
number, for each of the ResourceGroups that are currently having
traffic, and for which the traffic has changed significantly since the
last time it was reported (eg: ±10%). Each broker will also use these
updates to keep track of which brokers are communicating on various
ResourceGroups; hence, each broker that is active on a ResourceGroups
will mandatorily report its usage once in N cycles (value of N may be
configurable), even if the traffic has not changed significantly.

The update will be in the form of a ProtocolBuffer message published
on the internal topic. The format of the update will be like:

```
{
   broker : “broker-1.example.com”,
   usage : {
      “resource-group-1” : {
         topics: 1,
         publishedMsg : 100,
         publishedBytes : 100000,
      },
      “resource-group-2” : {
         topics: 1,
         publishedMsg : 1000,
         publishedBytes : 500000,
      },
      “resource-group-3” : {
         topics: 1,
         publishedMsg : 80000,
         publishedBytes : 9999999,
      },
   }
}
```

Each broker will use a Pulsar reader on the topic and will receive
every update from other brokers. These updates will get inserted into
a hash map:

```
Map<ResourceGroup, Map<BrokerName, Usage>>
```

With this, each broker will be aware of the actual usage done by each
broker on the particular resource group. It will then proceed to
adjust the rate on a local in-memory rate limiter, in the same way
we’re currently doing the per-topic rate limiting.

Example of usage distribution for a given ResourceGroup with a quota
of 100. Let’s assume that the quota-assignment of 100 to this
ResourceGroup is known to all the brokers (through configuration not
shown here).

 * broker-1: 10
 * broker-2:  50
 * broker-3: 30

In this case, each broker will adjust their own local limits to
utilize the remaining 10 units. They might each split up the remaining
portion, each adding the remaining 10 units:

 * broker-1 : 20
 * broker-2: 60
 * broker-3: 40

In the short term, this will lead to passing the set quota, but it
will quickly converge in just a few cycles to the fair values.

Alternatively, each broker may split up the 10 units proportionally,
based on historic usage (so they can use 1/9th, 5/9ths, and 1/3rd of
the residual 10 units).

 * broker-1 : 11.11
 * broker-2: 55.56
 * broker-3: 33.33
 
The opposite would happen (each broker would reduce its usage by the
corresponding fractional amount) if the recent-usage was over the
quota assigned on the resource-group.

In a similar way, brokers will try to “steal” part of the quota when
there is another broker using a bigger portion. For example, consider
the following usage report map:

 * broker-1: 80
 * broker-2:  20

Broker-2 has the rate limiter set to 20 and that also reflects the
actual usage and therefore could just mean that broker-2 is unfairly
throttled. Since broker-1 is dominant in the usage map, broker-2 will
set the local limiter to a value that is higher than 20, for example
half-way to the next broker, in this case to `20 + (80 - 20)/2 - 50`.

If indeed, broker-2 has more demand for traffic, that will increase
broker-2 usage to 30 in the next update and it consequently trigger
broker-1 to reduce its limit to 70. This step-by-step will continue
until it converges to the equilibrium point.

Generalizing it for the N brokers case, the broker with the lowest
quota will steal part of the quota of the most dominant broker. Broker
with second lowest quota will try to steal part of the quota of the
second dominant broker and so on till all brokers converge to the
equilibrium point.

#### Goal

Whenever an event that influences the quota allocation
(broker/producer/consumer joins or leaves) occurs, the quota
adjustment step function needs to converge the quotas to stable
allocations in minimum number of iterations, while also ensuring that:

 * The adjustment curve should be smooth instead of being jagged.
 * The quota is not under-utilized.
   - For example if the quota is 100 and there are two brokers and
      broker-1 is allocated 70, broker-2 is allocated 30. If
      broker-1's usage is 80 and broker-2's usage is 20 we need to
      ensure the design does not lead to under-utilization
 * Fairness of quota allocation across brokers.
   - If quota is 100 both brokers are seeing a uniform load of say 70,
     but one broker is allocated 70 and the other is allocated 30.


#### Clearing up stale broker entries

Brokers are only sending updates in the common topic if there are
significant changes, or if they have not reported for a (configurable)
number of rounds due to unchanged usage. This is to minimize the
amount of traffic in the common topic and work to be done to process
them.

When a broker publishes an update with a quota of 0, everyone will
remove that broker from the usage map. In the same way, when brokers
detect that one broker went down, through the ZooKeeper registration,
it will be clearing that broker from all the usage maps.

#### Rate limiting across topics/partitions

For each tenant/namespace that a broker is managing, the usage
reported by it is the aggregate of usages for all
topics/partitions. Therefore, the quota adjustment function will
divide the quota proportionally (taking the usages reported by other
brokers into consideration). And within the quota allocated to the
broker, it can choose to either sub-divide it evenly across it’s own
topics/partitions or sub-divide it proportional to the usages of each
topic/partition.

#### Avoidance of old messages

Because communication on the control topic is periodic, stale messages
(as determined from the publish time in the Pulsar metadata) can be
discarded by consumers.

### Resource consumption considerations

With the proposed implementation, each broker will keep a full map of
all the resource groups and their usage, broker by broker.  The amount
of memory/bandwidth consumption will depend on several factors such as
number of namespaces, brokers etc. Below is an approximate estimate
for one type of scaled scenario [where the quotas are enforced at a
namespace level].

In this scenario, let’s consider:
 * 100000 namespaces
 * 100 brokers.
 
Each namespace is spread across 5 brokers. So, each broker is managing 5000 namespaces.

#### Memory

For a given namespace, each broker stores usage from 5 other brokers
(including itself).

 * Size of usage = 16 bytes (bytes+messages)
 * Size of usage for publish+consume = 32bytes
 * For one namespace, usage of 5 brokers = 32*5
 * 5000 namespaces = 32*5*5000 = 800K

Meta-data overhead [Assuming that namespace name is about 80 bytes and
broker name is 40 bytes]: `5000*80 + 5 * 40 = 400K bytes`.

Total memory requirement ~= 1MB.

#### Bandwidth

Each broker sends the usage for the namespaces that it manages.

 * Size of usage = 16 bytes
 * Size of usage for publish+consume = 32bytes
 * For 5000 namespaces, each broker publishes periodically (say every
   minute): 32*5000 = 160K bytes.
 * Metadata overhead [assuming broker name is 40 bytes and namespace
   is 80 bytes]: 5000*80 = 400K.
 * For 100 brokers: (160K + 400K) * 100 = 56MB.

So, publish side network bandwidth is 56MB.
Including the consumption side (across all brokers), it is 56MB*100 = 5.6G.

Few optimizations that can reduce the overall bandwidth:

 * Brokers publish usage only if the usage is significantly different
   from the previous update.
 * Use compression (trade-off is higher CPU). 
 * Publish to more than one system topic (so the network load gets
   distributed across brokers).
 * Since metadata changes [ namespace/tenant addition/removal ] are
   rare, publish the metadata update [namespace name to ID mapping]
   only when there is a change. The Usage report will carry the
   namespace/tenant ID instead of the name.


#### Persistent storage

The usage data doesn’t require persistent storage. So, there is no
persistent storage overhead.


### ResourceGroup Configuration

#### Global Configuration

 * `resourceUsagePublishTopicName`: Name of the Topic to which `ResourceGroup` usage will be published, eg. `non-persistent://pulsar/system/resource-usage`
 * `resourceUsagePublishToTopic`: Boolean to enable/disable the `ResourceGroup` usage publish.
 * `resourceUsagePublishIntervalInSecs`: Interval cadence at which the `ResourceGroup` usage will be published on the above mentioned topic.


#### ResourceGroup REST-API

All APIs related to ResourceGroup will require admin privileges, this
keeps it simple and if we see a  need to change this in the future we
shall handle it at that time.

##### Create/update ResourceGroup

```
PUT admin/v2/resourcegroup/{resourcegroup}

Path Parameters

Resourcegroup - Name of the resourcegroup to create

Body

publishRateInmsgs  - Number of message per period
PublishRateInbytes - Number of bytes per period
DispatchRateInmsgs  - Number of messages per period
DispatchRateInbytes - Number of bytes per period


Responses


403 - Don't have admin permissions.
```

##### Get ResourceGroup detail

```
GET admin/v2/resourcegroup/{resourcegroup}

Path Parameters

Resourcegroup - Name of the resourcegroup to lookup

Body

Empty

Responses


200 - Successful operation.
403 - Don’t have admin permissions.

Response body on success


PublishRatelimitinmsgs  - Number of message per period
PublishRatelimitinbytes - Number of bytes per period
DispatchRatelimitinmsgs  - Number of messages per period
DispatchRatelimitinbytes - Number of bytes per period
```



##### Get List of ResourceGroups

```
GET admin/v2/resourcegroup/

Path Parameters

Empty

Body

Empty

Responses


200 - Successful operations.
403 - Don’t have admin permissions.

Response body on success

List of Resourcegroups.


Delete ResourceGroup
DELETE admin/v2/resourcegroup/{resourcegroup}

Path Parameters

Resourcegroup - Name of the resourcegroup to delete

Body

Empty

Responses


200 - success
403 - Don’t have admin permissions
```


##### Attach ResourceGroup to namespace
Question: Since v2 api of namespace create takes the policies in the body 
should we just add the resource group as one more parameter in the namespace PUT itself?

```
PUT admin/v2/namespaces/{tenant}/{namespace}/resourcegroup/{resourcegroup}

Path Parameters

Tenant - name of tenant
Namespace - name of namespace
Resourcegroup - name of resourcegroup

Body

Empty

Responses

403 - Don’t have admin permissions.
```

##### Detach ResourceGroup from namespace

```
DELETE admin/v2/namespaces/{tenant}/{namespace}/resourcegroup

Path Parameters

Tenant - name of tenant
Namespace - name of namespace

Body

Empty.

Responses

200 - Successful operation.
403 - Don’t have admin permissions.
```

##### Get ResourceGroup attached to namespace

```
GET admin/v2/namespaces/{tenant}/{namespace}/resourcegroup

Path Parameters

Tenant - name of tenant
Namespace - name of namespace

Body

Empty.

Responses

403 - Don’t have admin persmissions

Response body on success

Name of the Resourcegroup attached to this namespace
```

##### Attach ResourceGroup to tenant
##### Detach ResourceGroup from tenant
##### Get ResourceGroup from tenant

The above tenant APIs will be similar as defined for namespace.

## Alternative approaches

### External database

One approach is to use a distributed DB (such as Cassandra, redis,
memcached etc) to store the quota counter and usage counters. Quota
reservation/refresh can just be increment/decrement operations on the
quota counter. The approach may seem reasonable, but has a few issues:

 * Atomic increment/decrement operations on the distributed counter
   can incur significant latency overhead.
 * Dependency on external systems has a very high operational cost. It
   is yet another cluster that needs to be deployed, maintained,
   updated etc.

### Centralized implementation

One broker is designated as leader for a given resource group and the
quota allocation is computed by that broker. The allocation is then
disseminated to other brokers. “Exclusive producer” feature can be
used for this purpose. This approach has a few issues.

 * More complex implementation because of leader election.
 * If the leader dies, another needs to be elected which can lead to
   unnecessary latency.

### Using Zookeeper

Another possible approach would have been for the brokers to exchange
usage information through Zookeeper. This was not pursued because of
perceived scaling issues: with a large number of brokers and/or
namespaces/tenants to rate-limit, the size of messages and the high
volume of writes into Zookeeper could become a problem. Since
Zookeeper is already used in certain parts of Pulsar, it was decided
that we should not burden that subsystem with more work.
