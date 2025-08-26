# PIP-49: Permission levels and inheritance

- Status: Draft
- Author: [Xiaolong Ran](https://github.com/wolfstudy)([@wolf4j](https://twitter.com/wolf4j1))
- Pull request:
- Mailing list discussion:
- Release:

## Motivation

The current pulsar admin's permission mechanism is somewhat confusing. There are some commands that have unreasonable permission verification, which causes pulsar's permission management to be incorrectly applied in pulsar admin. 

This PIP proposes the following permission changes for each admin API. 

### clusters

Command |Current Permissions | Proposed Permissions
----|---|---
get | super-user | super-user 
create | super-user | super-user
update | super-user | super-user
delete | super-user | super-user
list | super-user | super-user
update-peer-clusters | super-user | super-user
get-peer-clusters | super-user | super-user
get-failure-domain | super-user | super-user
create-failure-domain | super-user | super-user
update-failure-domain | super-user | super-user
delete-failure-domain | super-user | super-user
list-failure-domain | super-user | super-user

### brokers

Command |Current Permissions | Proposed Permissions
----|---|---
list | super-user | super-user
namespaces | super-user | super-user
update-dynamic-config | super-user | super-user
delete-dynamic-config | super-user | super-user
list-dynamic-config | super-user | super-user
get-all-dynamic-config | super-user | super-user
get-internal-config | super-user | super-user 
get-runtime-config | super-user | super-user 
healthcheck | super-user | super-user

### broker-stats

Command |Current Permissions | Proposed Permissions
----|---|---
monitoring-metrics | super-user | super-user
mbeans | super-user | super-user
topics | super-user | super-user
allocator-stats | super-user | super-user
load-report | super-user | super-user

### functions-worker

Command |Current Permissions | Proposed Permissions
----|---|---
function-stats | super-user | super-user
monitoring-metrics | super-user | super-user
get-cluster | super-user | super-user
get-cluster-leader | super-user | super-user
get-function-assignments | super-user | super-user

### resource-quotas

Command |Current Permissions | Proposed Permissions
----|---|---
get | super-user | super-user
set | super-user | super-user
reset-namespace-bundle-quota | super-user | super-user

### ns-isolation-policy

Command |Current Permissions | Proposed Permissions
----|---|---
get | super-user | super-user
set | super-user | super-user
list | super-user | super-user
delete | super-user | super-user
brokers | super-user | super-user
broker | super-user | super-user

### tenants

Command |Current Permissions | Proposed Permissions
----|---|---
get | super-user | super-user
create | super-user | super user
update | super-user | super user
delete | super-user | super user
list | super-user | super user

### schemas

Command |Current Permissions | Proposed Permissions
----|---|---
get | tenant admin | **super user and tenant admin and produce**
upload | tenant admin | **super user and tenant admin and produce**
extract | tenant admin | **super user and tenant admin and produce**
delete | tenant admin | **super user and tenant admin and produce**


### functions

Command |Current Permissions | Proposed Permissions
----|---|---
localrun | none | none
create | super-user and tenant admin and function | super-user and tenant admin and function
delete | super-user and tenant admin and function | super-user and tenant admin and function
update | super-user and tenant admin and function | super-user and tenant admin and function
get | super-user and tenant admin and function | super-user and tenant admin and function
restart | super-user and tenant admin and function | super-user and tenant admin and function
stop | super-user and tenant admin and function | super-user and tenant admin and function
start | super-user and tenant admin and function | super-user and tenant admin and function
status | super-user and tenant admin and function | super-user and tenant admin and function
stats | super-user and tenant admin and function | super-user and tenant admin and function
list | super-user and tenant admin and function | super-user and tenant admin and function
querystate | super-user and tenant admin and function | super-user and tenant admin and function
trigger | super-user and tenant admin and function | super-user and tenant admin and function
putstate | super-user and tenant admin and function | super-user and tenant admin and function

### sources

Command |Current Permissions | Proposed Permissions
----|---|---
localrun | none | none
create | super-user and tenant admin and function | super-user and tenant admin and function
delete | super-user and tenant admin and function | super-user and tenant admin and function
update | super-user and tenant admin and function | super-user and tenant admin and function
get | none | **super-user and tenant admin and function**
status | super-user and tenant admin and function | super-user and tenant admin and function
stop | super-user and tenant admin and function | super-user and tenant admin and function
start | super-user and tenant admin and function | super-user and tenant admin and function
list | super-user and tenant admin and function | super-user and tenant admin and function
restart | super-user and tenant admin and function | super-user and tenant admin and function

### sinks

Command |Current Permissions | Proposed Permissions
----|---|---
localrun | none | none
create | super-user and tenant admin and function | super-user and tenant admin and function
delete | super-user and tenant admin and function | super-user and tenant admin and function
update | super-user and tenant admin and function | super-user and tenant admin and function
get | none | **super-user and tenant admin and function**
status | super-user and tenant admin and function | super-user and tenant admin and function
stop | super-user and tenant admin and function | super-user and tenant admin and function
start | super-user and tenant admin and function | super-user and tenant admin and function
list | super-user and tenant admin and function | super-user and tenant admin and function
restart | super-user and tenant admin and function| super-user and tenant admin and function

### topics

Command | Current Permissions | Proposed Permissions
----|---|---
compact | tenant admin | **super user and tenant admin**
compaction-status | tenant admin | **super user and tenant admin**
offload | tenant admin | **super user and tenant admin**
offload-status | tenant admin | **super user and tenant admin**
create-partitioned-topic | tenant admin | **super user and tenant admin**
delete-partitioned-topic | tenant admin | **super user and tenant admin**
create | tenant admin | **super user and tenant admin**
get-partitioned-topic-metadata | tenant admin | **super user and tenant admin and produce and consume**
update-partitioned-topic | tenant admin | **super user and tenant admin**
list | tenant admin | **super user and tenant admin**
terminate | tenant admin | **super user and tenant admin**
permissions | tenant admin | **super user and tenant admin**
grant-permission | tenant admin | **super user and tenant admin**
revoke-permission | tenant admin | **super user and tenant admin**
lookup | produce or consume | **super user and tenant admin and produce and consume**
bundle-range | super-user | super user
delete | tenant admin | **super user and tenant admin**
unload | super-user | super user
create-subscription | tenant admin | **super user and tenant admin and consume**
stats | tenant admin | **super user and tenant admin and produce and consume**
stats-internal | tenant admin | **super user and tenant admin and produce and consume**
info-internal | tenant admin | **super user and tenant admin and produce and consume**
partitioned-stats | tenant admin | **super user and tenant admin and produce and consume**
skip-all | tenant admin | **super user and tenant admin**
expire-messages-all-subscriptions | tenant admin | **super user and tenant admin**
last-message-id | tenant admin | **super user and tenant admin**
create-subscription | tenant admin and namespace produce or consume | **super user and tenant admin and produce and consume**
unsubscribe | tenant admin and consume | **super user and tenant admin and consume**
skip | tenant admin or consume | **super user and tenant admin and consume**
expire-messages | tenant admin and produce or consume | **super user and tenant admin and consume**
peek-messages | tenant admin and produce or consume | **super user and tenant admin and consume**
reset-cursor | tenant admin and produce or consume | **super user and tenant admin and consume**
subscriptions | tenant admin and produce or consume | **super user and tenant admin and consume**

### namespaces

Command |Current Permissions | Proposed Permissions
----|---|---
list | tenant admin | **super user and tenant admin**
topics | tenant admin | **super user and tenant admin**
policies | tenant admin | **super user and tenant admin**
create | tenant admin | **super user and tenant admin**
delete | tenant admin | **super user and tenant admin**
set-deduplication | tenant admin | **super user and tenant admin**
permissions | tenant admin | **super user and tenant admin**
grant-permissions | tenant admin | **super user and tenant admin**
revoke-permissions | tenant admin | **super user and tenant admin**
grant-subscription-permission | tenant admin | **super user and tenant admin**
revoke-subscription-permission | tenant admin | **super user and tenant admin**
set-clusters | tenant admin | **super user and tenant admin**
get-clusters | tenant admin | **super user and tenant admin**
get-backlog-quotas | tenant admin | **super user and tenant admin**
set-backlog-quota | tenant admin | **super user and tenant admin**
remove-backlog-quota | tenant admin | **super user and tenant admin**
get-persistence | tenant admin | **super user and tenant admin**
get-backlog-quotas | tenant admin | **super user and tenant admin**
set-backlog-quota | tenant admin | **super user and tenant admin**
remove-backlog-quota | tenant admin | **super user and tenant admin**
get-persistence | tenant admin | **super user and tenant admin**
set-persistence | tenant admin | **super user and tenant admin**
get-message-ttl | tenant admin | **super user and tenant admin**
set-message-ttl | tenant admin | **super user and tenant admin**
get-anti-affinity-group | tenant admin | **super user and tenant admin**
set-anti-affinity-group | tenant admin | **super user and tenant admin**
delete-anti-affinity-group | tenant admin | **super user and tenant admin**
get-anti-affinity-namespaces | tenant admin | **super user and tenant admin**
get-retention | tenant admin | **super user and tenant admin**
set-retention | tenant admin | **super user and tenant admin**
unload | super-user | super user
set-replicator-dispatch-rate | super-user |  super user
get-replicator-dispatch-rate | tenant admin | **super user and tenant admin**
split-bundle | super-user | super user
set-dispatch-rate | super-user | super user
get-dispatch-rate | tenant admin | **super user and tenant admin**
get-subscribe-rate | tenant admin | **super user and tenant admin**
set-subscribe-rate | super-user | super user
set-subscription-dispatch-rate | super-user | super user
get-subscription-dispatch-rate | tenant admin | **super user and tenant admin**
clear-backlog | tenant admin | **super user and tenant admin**
unsubscribe | tenant admin | **super user and tenant admin**
set-encryption-required | tenant admin | **super user and tenant admin**
set-subscription-auth-mode | tenant admin | **super user and tenant admin**
get-max-producers-per-topic | tenant admin | **super user and tenant admin**
set-max-producers-per-topic | super-user | super user
get-max-consumers-per-topic | tenant admin | **super user and tenant admin**
set-max-consumers-per-topic | super-user | super user
get-max-consumers-per-subscription | tenant admin |**super user and tenant admin**
get-compaction-threshold | tenant admin | **super user and tenant admin**
get-offload-threshold | tenant admin | **super user and tenant admin**
get-offload-deletion-lag | tenant admin | **super user and tenant admin**
get-schema-autoupdate-strategy | tenant admin | **super user and tenant admin**
get-schema-validation-enforced | tenant admin | **super user and tenant admin**
set-schema-autoupdate-strategy | super-user | super user
set-schema-validation-enforced | super-user | super user
set-offload-deletion-lag | super-user | super user
clear-offload-deletion-lag | super-user | super user
set-offload-threshold | super-user | super user
set-compaction-threshold | super-user | super user
set-max-consumers-per-subscription | super-user | super user
