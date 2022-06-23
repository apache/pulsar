---
id: admin-api-tenants
title: Managing Tenants
sidebar_label: "Tenants"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


> **Important**
>
> This page only shows **some frequently used operations**.
>
> - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/)
> 
> - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
> 
> - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

Tenants, like namespaces, can be managed using the [admin API](admin-api-overview). There are currently two configurable aspects of tenants:

* Admin roles
* Allowed clusters

## Tenant resources

### List

You can list all of the tenants associated with an [instance](reference-terminology.md#instance).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`list`](/tools/pulsar-admin/) subcommand.

```shell

$ pulsar-admin tenants list
my-tenant-1
my-tenant-2

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/tenants|operation/getTenants?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.tenants().getTenants();

```

</TabItem>

</Tabs>
````

### Create

You can create a new tenant.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`create`](/tools/pulsar-admin/) subcommand:

```shell

$ pulsar-admin tenants create my-tenant

```

When creating a tenant, you can optionally assign admin roles using the `-r`/`--admin-roles`
flag, and clusters using the `-c`/`--allowed-clusters` flag. You can specify multiple values
as a comma-separated list. Here are some examples:

```shell

$ pulsar-admin tenants create my-tenant \
  --admin-roles role1,role2,role3 \
  --allowed-clusters cluster1

$ pulsar-admin tenants create my-tenant \
  -r role1
  -c cluster1

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/tenants/:tenant|operation/createTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.tenants().createTenant(tenantName, tenantInfo);

```

</TabItem>

</Tabs>
````

### Get configuration

You can fetch the [configuration](reference-configuration) for an existing tenant at any time.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`get`](/tools/pulsar-admin/) subcommand and specify the name of the tenant. Here's an example:

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

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/tenants/:tenant|operation/getTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.tenants().getTenantInfo(tenantName);

```

</TabItem>

</Tabs>
````

### Delete

Tenants can be deleted from a Pulsar [instance](reference-terminology.md#instance).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`delete`](/tools/pulsar-admin/) subcommand and specify the name of the tenant.

```shell

$ pulsar-admin tenants delete my-tenant

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/tenants/:tenant|operation/deleteTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.Tenants().deleteTenant(tenantName);

```

</TabItem>

</Tabs>
````

### Update

You can update a tenant's configuration.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`update`](/tools/pulsar-admin/) subcommand.

```shell

$ pulsar-admin tenants update my-tenant

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/tenants/:tenant|operation/updateTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.tenants().updateTenant(tenantName, tenantInfo);

```

</TabItem>

</Tabs>
````
