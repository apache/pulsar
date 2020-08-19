---
id: version-2.6.2-admin-api-tenants
title: Managing Tenants
sidebar_label: Tenants
original_id: admin-api-tenants
---

Tenants, like namespaces, can be managed using the [admin API](admin-api-overview.md). There are currently two configurable aspects of tenants:

* Admin roles
* Allowed clusters

## Tenant resources

### List

#### pulsar-admin

You can list all of the tenants associated with an [instance](reference-terminology.md#instance) using the [`list`](reference-pulsar-admin.md#tenants-list) subcommand:

```shell
$ pulsar-admin tenants list
```

That will return a simple list, like this:

```
my-tenant-1
my-tenant-2
```

### Create

#### pulsar-admin

You can create a new tenant using the [`create`](reference-pulsar-admin.md#tenants-create) subcommand:

```shell
$ pulsar-admin tenants create my-tenant
```

When creating a tenant, you can assign admin roles using the `-r`/`--admin-roles` flag. You can specify multiple roles as a comma-separated list. Here are some examples:

```shell
$ pulsar-admin tenants create my-tenant \
  --admin-roles role1,role2,role3

$ pulsar-admin tenants create my-tenant \
  -r role1
```

### Get configuration

#### pulsar-admin

You can see a tenant's configuration as a JSON object using the [`get`](reference-pulsar-admin.md#tenants-get) subcommand and specifying the name of the tenant:

```shell
$ pulsar-admin tenants get my-tenant
{
  "adminRoles": [
    "admin1",
    "admin2"
  ],
  "allowedClusters": [
    "cl1",
    "cl2"
  ]
}
```

### Delete

#### pulsar-admin

You can delete a tenant using the [`delete`](reference-pulsar-admin.md#tenants-delete) subcommand and specifying the tenant name:

```shell
$ pulsar-admin tenants delete my-tenant
```

### Updating

#### pulsar-admin

You can update a tenant's configuration using the [`update`](reference-pulsar-admin.md#tenants-update) subcommand
