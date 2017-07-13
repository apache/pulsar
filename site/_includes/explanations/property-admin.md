Properties, like namespaces, can be managed using the [admin API](../../admin/AdminInterface). There are currently two configurable aspects of properties:

* Admin roles
* Allowed clusters

### List

#### pulsar-admin

You can list all of the properties associated with an {% popover instance %} using the [`list`](../../reference/CliTools#pulsar-admin-properties-list) subcommand:

```shell
$ pulsar-admin properties list
```

That will return a simple list, like this:

```
my-property-1
my-property-2
```

### Create

#### pulsar-admin

You can create a new property using the [`create`](../../reference/CliTools#pulsar-admin-properties-create) subcommand:

```shell
$ pulsar-admin properties create my-property
```

When creating a property, you can assign admin roles using the `-r`/`--admin-roles` flag. You can specify multiple roles as a comma-separated list. Here are some examples:

```shell
$ pulsar-admin properties create my-property \
  --admin-roles role1,role2,role3

$ pulsar-admin properties create my-property \
  -r role1
```

### Get configuration

#### pulsar-admin

You can see a property's configuration as a JSON object using the [`get`](../../reference/CliTools#pulsar-admin-properties-get) subcommand and specifying the name of the property:

```shell
$ pulsar-admin properties get my-property
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

#### pulsar-adnin

You can delete a property using the [`delete`](../../reference/CliTools#pulsar-admin-properties-delete) subcommand and specifying the property name:

```shell
$ pulsar-admin properties delete my-property
```

### Updating

#### pulsar-admin

You can update a property's configuration using the [`update`](../../reference/CliTools#pulsar-admin-properties-update) subcommand
