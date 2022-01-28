---
id: version-2.9.1-admin-api-tenants
title: Managing Tenants
sidebar_label: Tenants
original_id: admin-api-tenants
---

> **Important**
>
> This page only shows **some frequently used operations**.
>
> - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](https://pulsar.apache.org/tools/pulsar-admin/)
> 
> - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
> 
> - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](https://pulsar.apache.org/api/admin/).

Tenants, like namespaces, can be managed using the [admin API](admin-api-overview.md). There are currently two configurable aspects of tenants:

* Admin roles
* Allowed clusters

## Tenant resources

### List

You can list all of the tenants associated with an [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`list`](reference-pulsar-admin.md#tenants-list) subcommand.

```shell
$ pulsar-admin tenants list
my-tenant-1
my-tenant-2
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/tenants|operation/getTenants?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.tenants().getTenants();
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Create

You can create a new tenant.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`create`](reference-pulsar-admin.md#tenants-create) subcommand:

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
<!--REST API-->

{@inject: endpoint|POST|/admin/v2/tenants/:tenant|operation/createTenant?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.tenants().createTenant(tenantName, tenantInfo);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get configuration

You can fetch the [configuration](reference-configuration.md) for an existing tenant at any time.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`get`](reference-pulsar-admin.md#tenants-get) subcommand and specify the name of the tenant. Here's an example:

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
<!--REST API-->

{@inject: endpoint|GET|/admin/v2/tenants/:cluster|operation/getTenant?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.tenants().getTenantInfo(tenantName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Delete

Tenants can be deleted from a Pulsar [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`delete`](reference-pulsar-admin.md#tenants-delete) subcommand and specify the name of the tenant.

```shell
$ pulsar-admin tenants delete my-tenant
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v2/tenants/:cluster|operation/deleteTenant?version=[[pulsar:version_number]]}

<!--JAVA-->

```java
admin.Tenants().deleteTenant(tenantName);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Update

You can update a tenant's configuration.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`update`](reference-pulsar-admin.md#tenants-update) subcommand.

```shell
$ pulsar-admin tenants update my-tenant
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v2/tenants/:cluster|operation/updateTenant?version=[[pulsar:version_number]]}

<!--JAVA-->

```java

admin.tenants().updateTenant(tenantName, tenantInfo);
```

<!--END_DOCUSAURUS_CODE_TABS-->
