# PIP-7: Pulsar Failure domain and Anti affinity namespaces

* **Status**: Implemented
 * **Author**: [Rajan Dhabalia](https://github.com/rdhabalia)
 * **Pull Request**: [#896](https://github.com/apache/incubator-pulsar/pull/896)
  * **Issue**: [#840](https://github.com/apache/incubator-pulsar/issues/840)

## Motivation

While there are tremendous operational benefits of collapsing domains under a single umbrella, one thing that becomes more difficult is managing the impact of failures when they do occur. So, it would be always beneficiary to divide a single domain into multiple virtual domains that can serve as failure domains and increases the system availability in case one of the domain goes offline.

Pulsar is resilient to hardware failures, but MTTR is of the order of few seconds  after detection. Sensitive applications prefer to reduce the impact to their applications during the MTTR window by not subjecting their entire traffic to a single failure, whether it is due to the failure of a single broker, rack or  rolling broker restarts due to a release deployment.  Partial failure is preferred over complete failure. The simplest way to  reduce the impact to the application is to distribute its traffic across multiple  failure units (termed failure domains) such that failure of  any unit causes only a partial disruption of the application traffic.

In Pulsar, each cluster will have multiple pre-configured failure domains and each failure domain is a logical region that contains set of brokers. Creating these multiple failure domains in a cluster can be useful in many scenarios:

- **Deploy a patch or release to specific domain:**
Sometimes, there will be a need to deploy a change/patch which requires for set of clients (eg. some critical/high-traffic namespace requires immediate attention or bug-fix).

- **Rollout a new release domain by domain:** 
We can do new release roll out domain by domain which can make sure that other domains are always available if current domain is going through maintenance or deployment-rollout.

- **Support anti-affinity namespace group:** 
Sometimes application has multiple namespaces and wants one of them available all the time to avoid any downtime. In this case, we can distribute such namespaces to different failure domains. We will discuss this feature in details in next section.

Therefore, we want to introduce "Failure domain" and "Anti-Affinity namespace" in pulsar .

## Proposal 

## Pulsar failure domain

Pulsar failure domain is a logical domain under a Pulsar cluster. Each logical domain contains pre-configured list of brokers in it. Pulsar will have admin api to create a logical domains under a cluster and register list of brokers under those logical domains.

### How to create domain and register brokers
Broker will store domain configuration in global-zookeeper at path: `/admin/clusters/<my-cluster>/failureDomain/<domain-name>`
- It stores list of brokers under a specific domain
- One broker can’t be part of multiple domains and API validates it before adding broker into domain

### Admin api:
- **Create domain:** 
It creates a domain under a specific cluster
```
$ pulsar-admin clusters create-failure-domain <cluster-name> --domain-name <domain-name> --broker-list <broker-list-comma-separated>
```
- **Update domain:** It updates domain with list of brokers
```
$ pulsar-admin clusters update-failure-domain <cluster-name> --domain-name <domain-name> --broker-list <broker-list-comma-separated>
```
- **Get domain:** It provides domain configuration
```
$ pulsar-admin clusters get-failure-domain <cluster-name> --domain-name <domain-name>
```
- **List of domain:** It provides list of domains and configured brokers under that domain
```
$ pulsar-admin clusters list-failure-domains <cluster-name>
```
- **Delete domain:** It deletes given domain and its configuration
```
$ pulsar-admin clusters delete-failure-domain <cluster-name> --domain-name <domain-name>
```

## Anti-affinity namespace group

Sometimes application has multiple namespaces and we want one of them available all the time to avoid any downtime. In this case, these namespaces should be owned by different failure domains and different brokers so, if one of the failure domain is down (due to release rollout or brokers restart) then it will only disrupt namespaces that owned by that specific failure domain which is down and rest of the namespaces owned by other domains will remain available without any impact.

Therefore, such group of namespaces have anti-affinity to each other and together they make an anti-affinity-group which describes that all the namespaces that are part of this anti-affinity group have anti-affinity and load-balancer should try to place these namespaces to different failure domains. _**if there are more anti-affinity namespaces than failure domains then, load-balancer distributes namespaces evenly across all the domains and also every domain should distribute namespaces evenly across all the brokers under that domain.**_

For instance in figure 1: 
- Pulsar has 2 failure domains (Domain-1 and Domain-2) and each domain has 2 brokers in it. Now, we have one anti-affinity group which has 4 namespaces in it so, all 4 namespaces have anti-affinity to each other and load-balancer should try to place them to different failure domains.
- However, Pulsar has only 2 failure domains so, each domain will own 2 namespaces each. Also, load-balancer will try to distribute namespaces evenly across all the brokers in the same domain. Each domain has 2 brokers so, every broker owns one namespace from this anti-affinity namespace-group.
- So, finally in figure 1, we can see that domain-1 and domain-2 owns 2 namespaces each and all 4 brokers owns 1 namespace each.

![image](https://user-images.githubusercontent.com/2898254/31748935-8c62baba-b42b-11e7-856e-e315fbe2b340.png)
           
[Figure 1: anti-affinity namespace distribution across failure domains]

### Admin api:
An anti-affinity group will be a unique identifier that groups all the namespaces that have anti-affinity to each other. Namespaces with the same anti-affinity group are considered as anti-affinity namespaces.

An anti-affinity group is created automatically when the first namespace is assigned to the group.

Each namespace can belong to only one anti-affinity group. If a namespace with an existing anti-affinity assignment is  assigned to another anti-affinity group, the original assignment will be dropped.

(1) Assign a namespace to an  anti-affinity group:  It sets anti-affinity group name for a namespace. 
```	
$ pulsar-admin namespaces set-anti-affinity-group <namespace> --group <group-name>
```

(2) Get the anti-affinity group a namespace is bound to
```	
$ pulsar-admin namespaces get-anti-affinity-group <namespace> 
```

(3) Remove namespace from an anti-affinity group: It removes the namespace from the anti-affinity group 
```	
$ pulsar-admin namespaces delete-anti-affinity-group <namespace> 
```

(4) Get all namespaces assigned to a given anti-affinity group
```	
$ pulsar-admin namespaces get-anti-affinity-namespaces --cluster <cluster-name> --group <group-name>
```


### Broker changes:

**Namespace policies**
To describe anti-affinity between namespaces, we have to bind them under one anti-affinity group which indicates that all namespaces under this group have anti-affinity to each other. Therefore, we will introduce a new field `antiAffinityGroup` under a  namespace-policies.

**Load-balancer**
While assigning namespace-bundle ownership, load-balancer will first check the anti-affinity group name for this namespace and if it exists then load-balancer will get list of all namespaces which belong to same anti-affinity group. Once, load-balancer will retrieve list of anti-affinity namespaces that belong to this group, load-balancer will try to place them under different failure domains. 

As we described earlier, load-balancer will provide a best effort to distribute such namespaces to different failure domains but it does not give guarantee if we have more number of anti-affinity namespaces than number of failure domains.

If we add a new namespace to an existing anti-affinity group then load-balancer will not unload already loaded namespace bundles but load-balancer will make sure that newly coming lookup request considers this change.

Load-balancer's load-shedding task should not unload the namespace if none of the candidate-broker other than current broker is eligible (due to maintain uniform namespace-distribution across the failure-domains and brokers) to own the anti-affinity namespace.
