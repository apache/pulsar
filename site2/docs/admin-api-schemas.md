---
id: admin-api-schemas
title: Manage Schemas
sidebar_label: "Schemas"
---


````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

:::tip

This page only shows **some frequently used operations**.

- For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/)

- For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

- For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

:::

## Manage AutoUpdate strategy

### Enable AutoUpdate

To enable `AutoUpdate` on a namespace, you can use the `pulsar-admin` command.

```bash
bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable tenant/namespace
```

### Disable AutoUpdate 

To disable `AutoUpdate` on a namespace, you can use the `pulsar-admin` command.

```bash
bin/pulsar-admin namespaces set-is-allow-auto-update-schema --disable tenant/namespace
```

Once the `AutoUpdate` is disabled, you can only register a new schema using the `pulsar-admin` command.

### Adjust compatibility

To adjust the schema compatibility level on a namespace, you can use the `pulsar-admin` command.

```bash
bin/pulsar-admin namespaces set-schema-compatibility-strategy --compatibility <compatibility-level> tenant/namespace
```

## Schema validation

### Enable schema validation

To enable `schemaValidationEnforced` on a namespace, you can use the `pulsar-admin` command.

```bash
bin/pulsar-admin namespaces set-schema-validation-enforce --enable tenant/namespace
```

### Disable schema validation

To disable `schemaValidationEnforced` on a namespace, you can use the `pulsar-admin` command.

```bash
bin/pulsar-admin namespaces set-schema-validation-enforce --disable tenant/namespace
```

## Schema manual management

### Upload a schema

To upload (register) a new schema for a topic, you can use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `upload` subcommand.

```bash
pulsar-admin schemas upload --filename <schema-definition-file> <topic-name>
```

The `schema-definition-file` is in JSON format. 

```json
{
    "type": "<schema-type>",
    "schema": "<an-utf8-encoded-string-of-schema-definition-data>",
    "properties": {} // the properties associated with the schema
}
```

The `schema-definition-file` includes the following fields:

| Field |  Description | 
| --- | --- |
|  `type`  |   The schema type. | 
|  `schema`  |   The schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this field should be blank. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition. </li> | 
|  `properties`  |  The additional properties associated with the schema. | 

Here are examples of the `schema-definition-file` for a JSON schema.

**Example 1**

```json
{
    "type": "JSON",
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"file1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file3\",\"type\":[\"string\",\"null\"],\"default\":\"dfdf\"}]}",
    "properties": {}
}
```

**Example 2**

```json
{
    "type": "STRING",
    "schema": "",
    "properties": {
        "key1": "value1"
    }
}
```

</TabItem>
<TabItem value="REST API">

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/uploadSchema?version=@pulsar:version_number@}

The post payload is in JSON format.

```json
{
    "type": "<schema-type>",
    "schema": "<an-utf8-encoded-string-of-schema-definition-data>",
    "properties": {} // the properties associated with the schema
}
```

The post payload includes the following fields:

| Field |  Description | 
| --- | --- |
|  `type`  |   The schema type. | 
|  `schema`  |   The schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this field should be blank. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition. </li> | 
|  `properties`  |  The additional properties associated with the schema. |

</TabItem>
<TabItem value="Java Admin API">

```java
void createSchema(String topic, PostSchemaPayload schemaPayload)
```

The `PostSchemaPayload` includes the following fields:

| Field |  Description | 
| --- | --- |
|  `type`  |   The schema type. | 
|  `schema`  |   The schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this field should be blank. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition. </li> | 
|  `properties`  |  The additional properties associated with the schema. | 

Here is an example of `PostSchemaPayload`:

```java
PulsarAdmin admin = …;

PostSchemaPayload payload = new PostSchemaPayload();
payload.setType("INT8");
payload.setSchema("");

admin.createSchema("my-tenant/my-ns/my-topic", payload);
```

</TabItem>

</Tabs>
````

### Get a schema (latest)

To get the latest schema for a topic, you can use one of the following methods. 

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `get` subcommand.

```bash
pulsar-admin schemas get <topic-name>
```

Example output:

```json
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

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/getSchema?version=@pulsar:version_number@}

Here is an example of a response, which is returned in JSON format.

```json
{
    "version": "<the-version-number-of-the-schema>",
    "type": "<the-schema-type>",
    "timestamp": "<the-creation-timestamp-of-the-version-of-the-schema>",
    "data": "<an-utf8-encoded-string-of-schema-definition-data>",
    "properties": {} // the properties associated with the schema
}
```

The response includes the following fields:

| Field |  Description | 
| --- | --- |
|  `version`  |   The schema version, which is a long number. | 
|  `type`  |   The schema type. | 
|  `timestamp`  |   The timestamp of creating this version of schema. | 
|  `data`  |   The schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this field should be blank. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition. </li> | 
|  `properties`  |  The additional properties associated with the schema. |

</TabItem>
<TabItem value="Java Admin API">

```java
SchemaInfo createSchema(String topic)
```

The `SchemaInfo` includes the following fields:

| Field |  Description | 
| --- | --- |
|  `name`  |   The schema name. | 
|  `type`  |   The schema type. | 
|  `schema`  |   A byte array of the schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this byte array should be empty. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition converted to a byte array. </li> | 
|  `properties`  |  The additional properties associated with the schema. | 

Here is an example of `SchemaInfo`:

```java
PulsarAdmin admin = …;

SchemaInfo si = admin.getSchema("my-tenant/my-ns/my-topic");
```

</TabItem>

</Tabs>
````

### Get a schema (specific)

To get a specific version of a schema, you can use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `get` subcommand.

```bash
pulsar-admin schemas get <topic-name> --version=<version>
```

</TabItem>
<TabItem value="REST API">

Send a `GET` request to a schema endpoint: {@inject: endpoint|GET|/admin/v2/schemas/:tenant/:namespace/:topic/schema/:version|operation/getSchema?version=@pulsar:version_number@}

Here is an example of a response, which is returned in JSON format.

```json
{
    "version": "<the-version-number-of-the-schema>",
    "type": "<the-schema-type>",
    "timestamp": "<the-creation-timestamp-of-the-version-of-the-schema>",
    "data": "<an-utf8-encoded-string-of-schema-definition-data>",
    "properties": {} // the properties associated with the schema
}
```

The response includes the following fields:

| Field |  Description | 
| --- | --- |
|  `version`  |   The schema version, which is a long number. | 
|  `type`  |   The schema type. | 
|  `timestamp`  |   The timestamp of creating this version of schema. | 
|  `data`  |   The schema definition data, which is encoded in UTF 8 charset. <li>If the schema is a **primitive** schema, this field should be blank. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition. </li> | 
|  `properties`  |  The additional properties associated with the schema. |

</TabItem>
<TabItem value="Java Admin API">

```java
SchemaInfo createSchema(String topic, long version)
```

The `SchemaInfo` includes the following fields:

| Field |  Description | 
| --- | --- |
|  `name`  |  The schema name. | 
|  `type`  |  The schema type. | 
|  `schema`  |   A byte array of the schema definition data, which is encoded in UTF 8. <li>If the schema is a **primitive** schema, this byte array should be empty. </li><li>If the schema is a **struct** schema, this field should be a JSON string of the Avro schema definition converted to a byte array. </li> | 
|  `properties`  |  The additional properties associated with the schema. | 

Here is an example of `SchemaInfo`:

```java
PulsarAdmin admin = …;

SchemaInfo si = admin.getSchema("my-tenant/my-ns/my-topic", 1L);
```

</TabItem>

</Tabs>
````

### Extract a schema

To provide a schema via a topic, you can use the following method.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"}]}>

<TabItem value="Admin CLI">

Use the `extract` subcommand.

```bash
pulsar-admin schemas extract --classname <class-name> --jar <jar-path> --type <type-name>
```

</TabItem>

</Tabs>
````

### Delete a schema

To delete a schema for a topic, you can use one of the following methods.

:::note

In any case, the **delete** action deletes **all versions** of a schema registered for a topic.

:::

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `delete` subcommand.

```bash
pulsar-admin schemas delete <topic-name>
```

</TabItem>
<TabItem value="REST API">

Send a `DELETE` request to a schema endpoint: {@inject: endpoint|DELETE|/admin/v2/schemas/:tenant/:namespace/:topic/schema|operation/deleteSchema?version=@pulsar:version_number@} 

Here is an example of a response, which is returned in JSON format.

```json
{
    "version": "<the-latest-version-number-of-the-schema>",
}
```

The response includes the following field:

Field | Description |
---|---|
`version` | The schema version, which is a long number. |

</TabItem>
<TabItem value="Java Admin API">

```java
void deleteSchema(String topic)
```

Here is an example of deleting a schema.

```java
PulsarAdmin admin = …;

admin.deleteSchema("my-tenant/my-ns/my-topic");
```

</TabItem>

</Tabs>
````

## Set schema compatibility check strategy 

You can set [schema compatibility check strategy](schema-evolution-compatibility.md#schema-compatibility-check-strategy) at the topic, namespace or broker level. 

The schema compatibility check strategy set at different levels has priority: topic level > namespace level > broker level. 

- If you set the strategy at both topic and namespace levels, it uses the topic-level strategy. 

- If you set the strategy at both namespace and broker levels, it uses the namespace-level strategy.

- If you do not set the strategy at any level, it uses the `FULL` strategy. For all available values, see [here](schema-evolution-compatibility.md#schema-compatibility-check-strategy).


### Topic level

To set a schema compatibility check strategy at the topic level, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the [`pulsar-admin topicPolicies set-schema-compatibility-strategy`](/tools/pulsar-admin/) command. 

```shell
pulsar-admin topicPolicies set-schema-compatibility-strategy <strategy> <topicName>
```

</TabItem>
<TabItem value="REST API">

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v2/topics/:tenant/:namespace/:topic|operation/schemaCompatibilityStrategy?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
void setSchemaCompatibilityStrategy(String topic, SchemaCompatibilityStrategy strategy)
```

Here is an example of setting a schema compatibility check strategy at the topic level.

```java
PulsarAdmin admin = …;

admin.topicPolicies().setSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
```

</TabItem>

</Tabs>
````

To get the topic-level schema compatibility check strategy, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the [`pulsar-admin topicPolicies get-schema-compatibility-strategy`](/tools/pulsar-admin/) command. 

```shell
pulsar-admin topicPolicies get-schema-compatibility-strategy <topicName>
```

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic|operation/schemaCompatibilityStrategy?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String topic, boolean applied)
```

Here is an example of getting the topic-level schema compatibility check strategy.

```java
PulsarAdmin admin = …;

// get the current applied schema compatibility strategy
admin.topicPolicies().getSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", true);

// only get the schema compatibility strategy from topic policies
admin.topicPolicies().getSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", false);
```

</TabItem>

</Tabs>
````

To remove the topic-level schema compatibility check strategy, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the [`pulsar-admin topicPolicies remove-schema-compatibility-strategy`](/tools/pulsar-admin/) command. 

```shell
pulsar-admin topicPolicies remove-schema-compatibility-strategy <topicName>
```

</TabItem>
<TabItem value="REST API">

Send a `DELETE` request to this endpoint: {@inject: endpoint|DELETE|/admin/v2/topics/:tenant/:namespace/:topic|operation/schemaCompatibilityStrategy?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
void removeSchemaCompatibilityStrategy(String topic)
```

Here is an example of removing the topic-level schema compatibility check strategy.

```java
PulsarAdmin admin = …;

admin.removeSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic");
```

</TabItem>

</Tabs>
````

### Namespace level

You can set schema compatibility check strategy at namespace level using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the [`pulsar-admin namespaces set-schema-compatibility-strategy`](/tools/pulsar-admin/) command. 

```shell
pulsar-admin namespaces set-schema-compatibility-strategy options
```

</TabItem>
<TabItem value="REST API">

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace|operation/schemaCompatibilityStrategy?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

Use the [`setSchemaCompatibilityStrategy`](/api/admin/) method.

```java
admin.namespaces().setSchemaCompatibilityStrategy("test", SchemaCompatibilityStrategy.FULL);
```

</TabItem>

</Tabs>
````

### Broker level

You can set schema compatibility check strategy at broker level by setting `schemaCompatibilityStrategy` in `conf/broker.conf` or `conf/standalone.conf` file.

```conf
schemaCompatibilityStrategy=ALWAYS_INCOMPATIBLE
```