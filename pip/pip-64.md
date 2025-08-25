# PIP-64: Introduce REST endpoints for producing, consuming and reading messages

- Status: Proposal
- Author: Sijie Guo
- Pull Request: 
- Mailing List discussion:
- Release: 

## Motivation

Currently, Pulsar provides a REST endpoint for managing resources in a Pulsar cluster. Additionally it also provides methods to query the state for those resources. But it lacks the ability to produce, consume and read messages through the web service. It is a bit inconvenient for applications that wants to use HTTP to interact with Pulsar. This proposal proposes to introduce REST endpoints for producing, consuming and reading messages via the web service.

## Produce messages

### POST /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)

Produce messages to a topic, optionally specifying keys or partitions for the messages. If no partition is provided, one will be chosen based on the hash of the key. If no key is provided, the partition will be chosen for each message in a round-robin fashion. Schemas may be provided as the full schema encoded as a string, or, after the initial request may be provided as the schema version returned with the first response.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to produce the messages to

#### Request JSON Object:

- **schema_type** _(string)_: The schema type
- **schema_version** _(int)_: Version returned by a previous request using the same schema. This version corresponds to the version of the schema in schema registry
- **key_schema** _(object)_: key schema information (optional)
- **value_schema** _(object)_: value schema information (optional)

#### Request JSON Array of Objects:

- **messages**: A list of messages to produce to the topic.
- **messages[i].key** _(object)_: The message key, formatted according to the schema, or null to omit a key (optional)
- **messages[i].value** _(object)_: The message value, formatted according to the schema
- **messages[i].partition** _(int)_: The partition to publish the message to (optional)
- **messages[i].properties** _(map[string, string])_: The properties of the message (optional)
- **messages[i].eventTime** _(long)_: The event time of the message (optional)
- **messages[i].sequenceId** _(long)_: The sequence id of the message (optional)
- **messages[i].replicationClusters** _(array[string])_: The list of clusters to replicate (optional)
- **messages[i].disableReplication** _(bool)_: The flag to disable replication (optional)
- **messages[i].deliverAt** _(long)_: Deliver the message only at or after the specified absolute timestamp (optional)
- **messages[i].deliverAfterMs** _(long)_: Deliver the message only after the specified relative delay in milliseconds (optional)

#### Response JSON Object:

- **schema_version** _(int)_: The schema version used to produce messages, or null if the schema type is `bytes` or `none`.

#### Response JSON Array of Objects:

- **messageIds** _(object)_: List of message ids the messages were published to.
- **messageIds[i].partition** _(int)_: Partition the message was published to, or null if publishing the message failed
- **messageIds[i].messageId** _(string)_: The base64 encoded message id, or null if publishing the message failed
- **messageIds[i].error_code** _(long)_: An error code classifying the reason this operation failed, or null if it succeeded.
	- 1 - Non-retriable Pulsar exception
	- 2 - Retriable. Pulsar exception; the message might be sent successfully if retried
- **messageIds[i].error** _(string)_: An error message describing why the operation failed, or null if it succeeded

#### Status Codes:

- 404 Not Found
	- Error code 40401 - Topic not found
- 422 Unprocessable Entity
	- Error code 42201: Request requires key/value schema but does not include the `key_schema`  or `value_schema` fields
	- Error code 42202: Request requires a non-kv schema but does not include the `value_schema` field
	- Error code 42203: Request includes invalid key schema
	- Error code 42204: Request includes invalid value schema
	- Error code 42205: Request includes invalid messages


### POST /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/partitions/(int: partition_id)

Produce messages to one partition of a topic. Schemas may be provided as the full schema encoded as a string, or, after the initial request may be provided as the schema version returned with the first response.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to produce the messages to
- **partition_id** _(int)_: Partition to produce the messages to

#### Request JSON Object:

- **schema_type** _(string)_: The schema type
- **schema_version** _(int)_: Version returned by a previous request using the same schema. This version corresponds to the version of the schema in schema registry
- **key_schema** _(string)_: Full schema encoded as a string (e.g. JSON serialized schema info)
- **value_schema** _(string)_: Full schema encoded as a string (e.g. JSON serialized schema info)

#### Request JSON Array of Objects:

- **messages**: A list of messages to produce to the topic.
- **messages[i].key** _(object)_: The message key, formatted according to the schema, or null to omit a key (optional)
- **messages[i].value** _(object)_: The message value, formatted according to the schema
- **messages[i].partition** _(int)_: The partition to publish the message to (optional)
- **messages[i].properties** _(map[string, string])_: The properties of the message (optional)
- **messages[i].eventTime** _(long)_: The event time of the message (optional)
- **messages[i].sequenceId** _(long)_: The sequence id of the message (optional)
- **messages[i].replicationClusters** _(array[string])_: The list of clusters to replicate (optional)
- **messages[i].disableReplication** _(bool)_: The flag to disable replication (optional)
- **messages[i].deliverAt** _(long)_: Deliver the message only at or after the specified absolute timestamp (optional)
- **messages[i].deliverAfterMs** _(long)_: Deliver the message only after the specified relative delay in milliseconds (optional)

#### Response JSON Object:

- **schema_version** _(int)_: The schema version used to produce messages, or null if the schema type is `bytes` or `none`.

#### Response JSON Array of Objects:

- **messageIds** _(object)_: List of message ids the messages were published to.
- **messageIds[i].partition** _(int)_: Partition the message was published to, or null if publishing the message failed
- **messageIds[i].messageId** _(string)_: The base64 encoded message id, or null if publishing the message failed
- **messageIds[i].error_code** _(long)_: An error code classifying the reason this operation failed, or null if it succeeded.
	- 1 - Non-retriable Pulsar exception
	- 2 - Retriable. Pulsar exception; the message might be sent successfully if retried
- **messageIds[i].error** _(string)_: An error message describing why the operation failed, or null if it succeeded

#### Status Codes:

- 404 Not Found
	- Error code 40401 - Topic not found
- 422 Unprocessable Entity
	- Error code 42201: Request requires key/value schema but does not include the `key_schema`  or `value_schema` fields
	- Error code 42202: Request requires a non-kv schema but does not include the `value_schema` field
	- Error code 42203: Request includes invalid key schema
	- Error code 42204: Request includes invalid value schema
	- Error code 42205: Request includes invalid messages

## Read messages

### GET /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/partitions/(int: partition_id)/messages

Fetch messages from one partition of a topic. The format of the embedded data returned by this request is determined by the schema of the topic.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **partition_id** _(int)_: Partition to read the messages from

#### Query Parameters:

- **timeout**: Maximum amount of milliseconds the reader will spend fetching messages. Other parameters controlling actual time spent fetching messages. `max_messages` and `max_bytes`.
- **max_messages**: The maximum number of messages should be included in the response. This provides approximate control over the size of responses and the amount of memory required to store the decoded response.
- **max_bytes**: The maximum number of bytes of unencoded keys and values that should be included in the response. The provides approximate control over the size of responses and the amount of memory required to store the decoded response.
- **include_schema**: Flag to control whether to include schema information or not.

#### Response JSON Array of Schema Objects:

- **schemas** _(object)_: List of schemas of the returned messages.
- **schemas[i].type** _(string)_:  The type of the schema
- **schemas[i].version** _(int)_:  The version of the schema
- **schemas[i].data** (string): The base64 encoded schema data
- **schemas[I].properties** (map[string, string]): The properties of the schema

#### Response JSON Array of Message Objects:

- **messages** _(object)_: List of messages returned
- **messages[i].messageId** _(string)_: The base64 encoded message id
- **messages[i].key** _(object)_: The message key, formatted according to the schema, or null to omit a key (optional)
- **messages[i].value** _(object)_: The message value, formatted according to the schema
- **messages[i].partition** _(int)_: The partition to publish the message to (optional)
- **messages[i].properties** _(map[string, string])_: The properties of the message (optional)
- **messages[i].eventTime** _(long)_: The event time of the message (optional)
- **messages[i].sequenceId** _(long)_: The sequence id of the message (optional)
- **messages[i].replicationClusters** _(array[string])_: The list of clusters to replicate (optional)

#### Status Codes:

- 404 Not Found
	- Error code 40401 - Topic not found

## Consume messages

The consumers resource provides access to the current state of subscription, allows you to create a consumer in a subscription and consume messages from topics and partitions.

Because consumers are stateful, any consumer created with the REST API are tied to a specific broker. A full URL is provided when the instance is created and it should be used to construct any subsequent requests. Failing to use the returned URL for future consumer requests will result in _404_ errors because the consumer will not be found.

### POST /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/subscription/(string: subscription)

Create a new consumer instance in the subscription.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **subscription** _(string)_: Name of the subscription

#### Request JSON Object:

- **schema_type** _(string)_: The schema type
- **schema_version** _(int)_: Version returned by a previous creation request using the same schema. This version corresponds to the version of the schema in schema registry
- **key_schema** _(object)_: key schema information (optional)
- **value_schema** _(object)_: value schema information (optional)
- **name** _(string)_ : Name of the consumer (optional)
- **id** _(string)_: ID of the consumer. Id returned by a previous creation request.  (optional)

#### Response JSON Object:

- **id** _(string)_ - Unique ID for the consumer instance in the subscription
- **base_uri** _(string)_ - Base URI used to construct URIs for subsequent requests against this consumer instance.

#### Status Codes:

- 409 Conflict
	- Error code - Consumer with the specified name already exists

### GET /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/subscription/(string: subscription)/consumer/(string: consumer_id)/messages

Fetch messages from a given consumer. The format of the embedded data returned by this request is determined by the schema of the topic.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **subscription** _(string)_: Name of the subscription
- **consumer_id** _(string)_: ID of the consumer returned from the creation request

#### Query Parameters:

- **timeout**: Maximum amount of milliseconds the reader will spend fetching messages. Other parameters controlling actual time spent fetching messages. `max_messages` and `max_bytes`.
- **max_messages**: The maximum number of messages should be included in the response. This provides approximate control over the size of responses and the amount of memory required to store the decoded response.
- **max_bytes**: The maximum number of bytes of unencoded keys and values that should be included in the response. The provides approximate control over the size of responses and the amount of memory required to store the decoded response.

#### Response JSON Array of Message Objects:

- **messages** _(object)_: List of messages returned
- **messages[i].messageId** _(string)_: The base64 encoded message id
- **messages[i].key** _(object)_: The message key, formatted according to the schema, or null to omit a key (optional)
- **messages[i].value** _(object)_: The message value, formatted according to the schema
- **messages[i].partition** _(int)_: The partition to publish the message to (optional)
- **messages[i].properties** _(map[string, string])_: The properties of the message (optional)
- **messages[i].eventTime** _(long)_: The event time of the message (optional)
- **messages[i].sequenceId** _(long)_: The sequence id of the message (optional)
- **messages[i].replicationClusters** _(array[string])_: The list of clusters to replicate (optional)

#### Status Codes:

- 404 Not Found
	- Error code 40401 - Topic not found


### DELETE /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/subscription/(string: subscription)/consumers/(string: consumer_id)

Remove the consumer from the subscription.

Note that this request must be made to the specific broker holding the consumer instance.

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **subscription** _(string)_: Name of the subscription
- **consumer_id** _(string)_: The id of the consumer

#### Status Codes:

- 404 Conflict
	- Error code 40403 - Consumer instance not found

### GET /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/subscription/(string: subscription)/cursor

Get the current cursor for a given subscription.

Note: this request must be made to the specific broker that owns the topic

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **subscription** _(string)_: Name of the subscription

#### Response JSON object:

- **cursor** _(object)_: The cursor object

### POST /topics/(string: tenant_name)/(string: namespace_name)/(string: topic_name)/subscription/(string: subscription)/cursor

Update the current cursor for a given subscription.

Note: this request must be made to the specific broker that owns the topic

#### Parameters

- **tenant_name** _(string)_: The tenant name
- **namespace_name** _(string)_: The namespace name
- **topic_name** _(string)_: Name of the topic to read the messages from
- **subscription** _(string)_: Name of the subscription

#### Request JSON object:

- **type** _(string)_: acknowledgement type

#### Response JSON Array of Message Objects:

- **messageIds** _(object)_: List of message ids to acknowledge.
- **messageIds[i]** _(string)_: The base64 encoded message id, or null if publishing the message failed

## Compatibility, Deprecation and Migration Plan

This PIP is introducing new features. It doesn’t change or remove any existing endpoints. So there is nothing to deprecate or migrate.

## Test Plan

- Unit tests
- Integration tests

## Rejected Alternatives

N/A
