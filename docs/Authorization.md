
# Pulsar Authorization

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Authorization model](#authorization-model)
- [Creating a new property](#creating-a-new-property)
- [Managing namespaces](#managing-namespaces)
- [Super users](#super-users)

<!-- /TOC -->

## Authorization model

In Pulsar, the authentication provider is charged to identify a particular
client and associate it with a ***role*** token.

A role is a string that might represent a single or multiple clients,
and these clients can be granted the permission to produce or consume
from certain topics, or to administer the configuration for a certain
[property](Architecture.md##property-and-namespace).

## Creating a new property

As we said, a Pulsar property identifies a tenant and typically is
provisioned by Pulsar instance administrators or by some kind of
self-service portal.

```shell
$ bin/pulsar-admin properties create my-property   \
        --admin-roles my-admin-role                \
        --allowed-clusters us-west,us-east
```

This command will create a new property `my-property` that will be
allowed to use the clusters `us-west` and `us-east`.

A client that successfully identify itself with the role `my-admin-role`,
will be allowed to do all the administration on this property.

## Managing namespaces

The property administrator will now be able to create multiple namespaces,
in the specified clusters.

```
$ bin/pulsar-admin namespaces create my-property/us-west/my-namespace
```

Once created, we can grant permissions to use this namespace:

```
$ bin/pulsar-admin namespaces grant-permission \
                        my-property/us-west/my-namespace \
                        --role my-client-role \
                        --actions produce,consume
```

After this, clients identifying with the role `my-client-role`, will be
able to use topics in the specified namespace.

## Super users

In Pulsar you can assign certain roles to be *super-users* of the system.

A super-user is allowed to do all administrative tasks on all properties
and namespaces, as well as publishing and subscribing to all topics.

Super users are configured in the broker conf file `conf/broker.conf`:

```shell
superUserRoles=my-super-user-1,my-super-user-2
```

Typically, super users role are used for administrators clients and also
for broker-to-broker authorization. In geo-replication every broker
needs to be able to publish in other clusters topics.
