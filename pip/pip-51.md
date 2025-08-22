# PIP-51: Tenant policy support

- Status: Draft
- Author: Alexandre DUVAL
- Pull request: 
- Mailing list discussion: https://lists.apache.org/thread.html/a937326861b8e49fdb9fc8982010f41fb978a88311ebeb0f24bb695f@%3Cdev.pulsar.apache.org%3E
- Release:

## Motivation

Pulsar quotas, retentions and other policies are defined on namespaces level and only enforced at topic level. It would be great to have global policies on tenant level which can be overriden if policies are lower in namespaces level.

The main goal is to provide a tenant for external users and a the way to define namespaces and so on with tenant level policies defined by the tenant provider.

## Proposed changes

The tenant's adminRoles property would be used to define the tenant's level policies. Then we will need to add a tenant userRoles which should be able to create namespaces and everything in the tenant that does not exceed the tenants level policies.

Then the namespaces policies should inherit from tenant policies with a global verifier for all namespaces to not exceed tenant policies.

It wouldn't be too difficult for retention and storage part. We would have to just check periodically on the already supplied brokers load report and get the overall namespace/tenant quota. If quotas exceed, then block producers only (not sure delete exceeded data because of the async block part is a good idea).
