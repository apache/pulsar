Permissions in Pulsar are managed at the {% popover namespace %} level (that is, within {% popover properties %} and {% popover clusters %}).

### Grant permissions

You can grant permissions to specific roles for lists of operations such as `produce` and `consume`.

#### pulsar-admin

Use the [`grant-permission`](../../reference/CliTools#pulsar-admin-namespaces-grant-permission) subcommand and specify a namespace, actions using the `--actions` flag, and a role using the `--role` flag:

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
  --actions produce,consume \
  --role admin10
```

Wildcard authorization can be performed when `authorizationAllowWildcardsMatching` is set to `true` in `broker.conf`.

e.g.
```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role 'my.role.*'
```

Then, roles `my.role.1`, `my.role.2`, `my.role.foo`, `my.role.bar`, etc. can produce and consume.  

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role '*.role.my'
```

Then, roles `1.role.my`, `2.role.my`, `foo.role.my`, `bar.role.my`, etc. can produce and consume.

**Note**: A wildcard matching works at **the beginning or end of the role name only**.

e.g.
```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role 'my.*.role'
```

In this case, only the role `my.*.role` has permissions.  
Roles `my.1.role`, `my.2.role`, `my.foo.role`, `my.bar.role`, etc. **cannot** produce and consume.

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
```

### Get permission

You can see which permissions have been granted to which roles in a namespace.

#### pulsar-admin

Use the [`permissions`](../../reference/CliTools#pulsar-admin-namespaces-permissions) subcommand and specify a namespace:

```shell
$ pulsar-admin namespaces permissions test-property/cl1/ns1
{
  "admin10": [
    "produce",
    "consume"
  ]
}   
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/permissions %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions)

#### Java

```java
admin.namespaces().getPermissions(namespace);
```

### Revoke permissions

You can revoke permissions from specific roles, which means that those roles will no longer have access to the specified namespace.

#### pulsar-admin

Use the [`revoke-permission`](../../reference/CliTools#pulsar-admin-revoke-permission) subcommand and specify a namespace and a role using the `--role` flag:

```shell
$ pulsar-admin namespaces revoke-permission test-property/cl1/ns1 \
  --role admin10
```

#### REST API

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role);
```
