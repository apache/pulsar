---
id: version-2.3.0-incubating-admin-api-schemas
title: Managing Schemas
sidebar_label: Schemas
original_id: admin-api-schemas
---

Schemas, like other entities in Pulsar, can be managed using the [admin API](admin-api-overview.md). 

## Schema resources

A Pulsar schema is a fairly simple data structure stored in Pulsar for representing the structure of messages stored in a Pulsar topic. The schema structure consists of:

- *Name*: A schema's name is the topic that the schema is associated to.
- *Type*: A schema type represents the type of the schema. The predefined schema types can be found [here](concepts-schema-registry.md#supported-schema-formats). If it 
  is a customized schema, it is left as an empty string.
- *Payload*: It is a binary representation of the schema. How to interpret it is up to the implementation of the schema.
- *Properties*: It is a user defined properties as a string/string map. Applications can use this bag for carrying any application specific logics. Possible properties
  might be the Git hash associated with the schema, an environment string like `dev` or `prod`, etc.

All the schemas are versioned with versions. So you can retrieve the schema definition of a given version if the version is not deleted.

### Upload Schema

#### pulsar-admin

You can upload a new schema using the [`upload`](reference-pulsar-admin.md#get-5) subcommand:

```shell
$ pulsar-admin schemas upload <topic-name> --filename /path/to/schema-definition-file 
```

The schema definition file should contain following json string on defining how the schema look like:

```json
{
    "type": "STRING",
    "schema": "",
    "properties": {
        "key1" : "value1"
    }
}
```

An example of the schema definition file can be found at {@inject: github:SchemaExample:/conf/schema_example.conf}.

#### REST

{@inject: endpoint|POST|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/uploadSchema?version=[[pulsar:version_number]]}

### Get Schema

#### pulsar-admin

You can get the latest version of Schema using the [`get`](reference-pulsar-admin.md#get-5) subcommand.

```shell
$ pulsar-admin schemas get <topic-name>
{
    "version": 0,
    "type": "String",
    "timestamp": 0,
    "data": "string",
    "properties": {
        "property1": "string",
        "property2": "string"
    }
}
```

You can also retrieve the Schema of a given version by specifying `--version` option.

```shell
$ pulsar-admin schemas get <topic-name> --version <version>
```

#### REST API

Retrieve the latest version of the schema:

{@inject: endpoint|GET|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/getSchema?version=[[pulsar:version_number]]}

Retrieve the schema of a given version:

{@inject: endpoint|GET|/admin/v2/schemas/:tenant/:namespace/:topic/schema/:version|operation/getSchema?version=[[pulsar:version_number]]}

### Delete Schema

#### pulsar-admin

You can delete a schema using the [`delete`](reference-pulsar-admin.md#delete-8) subcommand.

```shell
$ pulsar-admin schemas delete <topic-name>
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/deleteSchema?version=[[pulsar:version_number]]}
