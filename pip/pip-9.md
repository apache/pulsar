# PIP-9: Adding more Security checks to Pulsar Proxy

**Status:** Adopted
**Author:** [Jai Asher](https://github.com/Jai1)
**Pull Request:** [#1002](https://github.com/apache/incubator-pulsar/pull/1002)

**Reference:** https://github.com/apache/incubator-pulsar/wiki/PIP-1:-Pulsar-Proxy

## Problem Description:
The machine hosting the Pulsar proxy will have a public IP and susceptible to all kinds of web attacks. The aim of this PIP is to minimize the damage caused by a compromised proxy on the entire service. Specifically, we want to make the following changes:-

### a. Access to zookeeper
The current implementation of Pulsar proxy requires access to zookeeper in order to find the next available broker and Authorize the client. If the Proxy is compromised and the attacker has access to zookeeper machines then the entire service is compromised and can be brought down. In order to prevent this, the proxy will no longer talk directly to zookeeper. 

### b. Limit proxy access to specific topics only
Currently, if a proxy is compromised and the attacker has knowledge of the Athens principal name of a topic/admin - the proxy can produce/consume from any topic. In order to prevent this, the proxy will be authorized to access only specific topic (AuthAction - proxy). In the worse case scenario if the compromised and the attacker has knowledge of the Athens principal name of a topic/admin - the proxy can produce/consume from only specific topics - just like any other client.

## Proposed Solution:
Following has been done to make the Pulsar Proxy more secure:-

### a. Make discovery service optional 
The proxy can now connect to broker service URL for lookups if discovery service is disabled in the proxy.

### b. Proxy AuthAction
A new auth action is added which allows admin to restrict whether a topic is proxyable or not

### c. Delegate Authorization to broker
The proxy only authenticates the client and passes along the client's role/principal name (athens) to the broker. The broker authenticates and authorizes the proxy _and_ authorizes the client role/principal name.

## Code Changes:-
### a. ServerCnx.java
   - During Producer/Consumer Creation - if the request is via a proxy the proxy is authorized as well as the original principal
   - During lookups and getPartitionMetaData - since the proxy uses shared connection pool for lookup and getPartitionMetaData we will pass the original principal as a part of the request and authorize the proxy and client role/principal name.
### b. Pulsar Protobuf
   - Added a new optional field original principal to the lookup and getPartitionMetaData request.
### c. LookupViaServiceUrl.java
   - New class to handle lookups via discovery service url
