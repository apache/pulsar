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

- For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/).

- For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

- For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

:::

## Manage schema

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

The following is an example of the `schema-definition-file` for a JSON schema.

**Example**

```json
{
    "type": "JSON",
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"file1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file3\",\"type\":[\"string\",\"null\"],\"default\":\"dfdf\"}]}",
    "properties": {}
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

</TabItem>
<TabItem value="Java Admin API">

```java
void createSchema(String topic, PostSchemaPayload schemaPayload)
```

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

### Get the latest schema

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

</TabItem>
<TabItem value="Java Admin API">

```java
SchemaInfo createSchema(String topic)
```

Here is an example of `SchemaInfo`:

```java
PulsarAdmin admin = …;

SchemaInfo si = admin.getSchema("my-tenant/my-ns/my-topic");
```

</TabItem>
</Tabs>
````

### Get a specific schema

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

</TabItem>
<TabItem value="Java Admin API">

```java
SchemaInfo createSchema(String topic, long version)
```

Here is an example of `SchemaInfo`:

```java
PulsarAdmin admin = …;

SchemaInfo si = admin.getSchema("my-tenant/my-ns/my-topic", 1L);
```

</TabItem>
</Tabs>
````

### Extract a schema

To extract (provide) a schema via a topic, use the following method.

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

:::note

In any case, the `delete` action deletes **all versions** of a schema registered for a topic.

:::

To delete a schema for a topic, you can use one of the following methods.

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

Here is an example of a response returned in JSON format.

```json
{
    "version": "<the-latest-version-number-of-the-schema>",
}
```

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

## Manage schema AutoUpdate

### Enable schema AutoUpdate

To enable/enforce schema auto-update at the namespace level, you can use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `set-is-allow-auto-update-schema` subcommand. 

```bash
bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable tenant/namespace
```

</TabItem>
<TabItem value="REST API">

Send a `POST` request to a namespace endpoint: {@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/isAllowAutoUpdateSchema|operation/isAllowAutoUpdateSchema?version=@pulsar:version_number@}

The post payload is in JSON format.

```json
{
“isAllowAutoUpdateSchema”: “true”
}
```

</TabItem>
<TabItem value="Java Admin API">

Here is an example to enable schema auto-update for a tenant/namespace.

```java
admin.namespaces().setIsAllowAutoUpdateSchema("my-namspace", true);
```

</TabItem>
</Tabs>
````

### Disable schema AutoUpdate

:::note

When schema auto-update is disabled, you can only [register a new schema](#upload-a-schema).

:::

To disable schema auto-update at the **namespace** level, you can use one of the following commands.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `set-is-allow-auto-update-schema` subcommand. 

```bash
bin/pulsar-admin namespaces set-is-allow-auto-update-schema --disable tenant/namespace
```

</TabItem>
<TabItem value="REST API">

Send a `POST` request to a namespace endpoint: {@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/isAllowAutoUpdateSchema|operation/isAllowAutoUpdateSchema?version=@pulsar:version_number@}

The post payload is in JSON format.

```json
{
“isAllowAutoUpdateSchema”: “false”
}
```

</TabItem>
<TabItem value="Java Admin API">

Here is an example to enable schema auto-unpdate of a tenant/namespace.

```java
admin.namespaces().setIsAllowAutoUpdateSchema("my-namspace", false);
```

</TabItem>
</Tabs>
````

## Manage schema validation enforcement

### Enable schema validation enforcement

To enforce schema validation enforcement at the **cluster** level, you can configure `schemaValidationEnforced` to `true` in the `conf/broker.conf` file. 

To enable schema validation enforcement at the **namespace** level, you can use one of the following commands.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `set-schema-validation-enforce` subcommand. 

```bash
bin/pulsar-admin namespaces set-schema-validation-enforce --enable tenant/namespace
```

</TabItem>
<TabItem value="REST API">

Send a `POST` request to a namespace endpoint: {@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/schemaValidationEnforced|operation/schemaValidationEnforced?version=@pulsar:version_number@}

The post payload is in JSON format.

```json
{
“schemaValidationEnforced”: “true”
}
```

</TabItem>
<TabItem value="Java Admin API">

Here is an example to enable schema validation enforcement for a tenant/namespace.

```java
admin.namespaces().setSchemaValidationEnforced("my-namspace", true);
```

</TabItem>
</Tabs>
````

### Disable schema validation enforcement

To disable schema validation enforcement at the **namespace** level, you can use one of the following commands.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `set-schema-validation-enforce` subcommand. 

```bash
bin/pulsar-admin namespaces set-schema-validation-enforce --disable tenant/namespace
```

</TabItem>
<TabItem value="REST API">

Send a `POST` request to a namespace endpoint: {@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/schemaValidationEnforced|operation/schemaValidationEnforced?version=@pulsar:version_number@}

The post payload is in JSON format.

```json
{
“schemaValidationEnforced”: “false”
}
```

</TabItem>
<TabItem value="Java Admin API">

Here is an example to enable schema validation enforcement for a tenant/namespace.

```java
admin.namespaces().setSchemaValidationEnforced("my-namspace", false);
```

</TabItem>
</Tabs>
````

## Manage schema compatibility strategy 

The [schema compatibility check strategy](schema-understand.md#schema-compatibility-check-strategy) configured at different levels has priority: topic level > namespace level > cluster level. In other words:
  * If you set the strategy at both topic and namespace levels, the topic-level strategy is used. 
  * If you set the strategy at both namespace and cluster levels, the namespace-level strategy is used. 

### Set schema compatibility strategy

#### Set topic-level schema compatibility strategy

To set a schema compatibility check strategy at the topic level, you can use one of the following methods.

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

#### Set namespace-level schema compatibility strategy

To set schema compatibility check strategy at the namespace level, you can use one of the following methods.

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

#### Set cluster-level schema compatibility strategy

To set schema compatibility check strategy at the **cluster** level, set `schemaCompatibilityStrategy` in the `conf/broker.conf` file.

The following is an example:

```conf
schemaCompatibilityStrategy=ALWAYS_INCOMPATIBLE
```

### Get schema compatibility strategy

#### Get topic-level schema compatibility strategy

To get the topic-level schema compatibility check strategy, you can use one of the following methods.

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

#### Get namespace-level schema compatibility strategy

You can get schema compatibility check strategy at namespace level using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the [`pulsar-admin namespaces get-schema-compatibility-strategy`](/tools/pulsar-admin/) command. 

```shell
pulsar-admin namespaces get-schema-compatibility-strategy options
```

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace|operation/schemaCompatibilityStrategy?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

Use the [`getSchemaCompatibilityStrategy`](/api/admin/) method.

```java
admin.namespaces().getSchemaCompatibilityStrategy("test", SchemaCompatibilityStrategy.FULL);
```

</TabItem>
</Tabs>
````