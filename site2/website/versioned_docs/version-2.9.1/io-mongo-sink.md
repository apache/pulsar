---
id: version-2.9.1-io-mongo-sink
title: MongoDB sink connector
sidebar_label: MongoDB sink connector
original_id: io-mongo-sink
---

The MongoDB sink connector pulls messages from Pulsar topics 
and persists the messages to collections.

## Configuration

The configuration of the MongoDB sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `mongoUri` | String| true| " " (empty string) | The MongoDB URI to which the connector connects. <br><br>For more information, see [connection string URI format](https://docs.mongodb.com/manual/reference/connection-string/). |
| `database` | String| true| " " (empty string)| The database name to which the collection belongs. |
| `collection` | String| true| " " (empty string)| The collection name to which the connector writes messages. |
| `batchSize` | int|false|100 | The batch size of writing messages to collections. |
| `batchTimeMs` |long|false|1000| The batch operation interval in milliseconds. |


### Example

Before using the Mongo sink connector, you need to create a configuration file through one of the following methods.

* JSON
  
    ```json
    {
        "mongoUri": "mongodb://localhost:27017",
        "database": "pulsar",
        "collection": "messages",
        "batchSize": "2",
        "batchTimeMs": "500"
    }
    ```

* YAML
  
    ```yaml
    configs:
        mongoUri: "mongodb://localhost:27017"
        database: "pulsar"
        collection: "messages"
        batchSize: 2
        batchTimeMs: 500
    ```

## Usage

This example shows how to sink the data to a MongoDB table using the Pulsar MongoDB connector.

1. Start a MongoDB server, then create a database and a collection.

    ```shell
    mongo

    > use local
    > rs.initiate()
    > use pulsar
    > db.createCollection(function)
    ```

2. Create a `mongo-sink.yaml` config file.

    ```yaml
    configs:
        mongoUri: "mongodb://127.0.0.1:27017"
        database: "pulsar"
        collection: "function"
    ```

3. Create the sink job. Make sure the NAR file is available at `connectors/pulsar-io-mongo-{{pulsar:version}}.nar`.

    ```shell
    bin/pulsar-admin sinks localrun \
    --archive connectors/pulsar-io-mongo-{{pulsar:version}}.nar \
    --tenant public \
    --namespace default \
    --inputs mongo-sink-topic \
    --name pulsar-mongo-sink \
    --sink-config-file mongo-sink.yaml  \
    --parallelism 1

4. Start the built-in connector DataGeneratorSource and ingest some mock data.Make sure the NAR file is available at `connectors/pulsar-io-data-generator-{{pulsar:version}}.nar`.

    ```shell
    bin/pulsar-admin sources create --name generator --destinationTopicName mongo-sink-topic --source-type data-generator
    ```

5. Check documents in the MongoDB.

    ```shell
    mongo

    > use pulsar
    > rs0:PRIMARY> db.function.find().pretty().limit(1)
    {
        "_id" : ObjectId("6214f32b1f31a4132b552397"),
        "address" : {
            "street" : "Herzi Street",
            "streetNumber" : "37",
            "apartmentNumber" : "193",
            "postalCode" : "73013",
            "city" : "New York"
        },
        "firstName" : "Jordan",
        "middleName" : "Samuel",
        "lastName" : "Romero",
        "email" : "jordan.romero@gmail.com",
        "username" : "jromero",
        "password" : "wrXRlRj6",
        "sex" : "MALE",
        "telephoneNumber" : "287-375-324",
        "dateOfBirth" : NumberLong("713018814903"),
        "age" : 29,
        "company" : {
            "name" : "Alist",
            "domain" : "alist.eu",
            "email" : "info@alist.eu",
            "vatIdentificationNumber" : "05-0005784"
        },
        "companyEmail" : "jordan.romero@alist.eu",
        "nationalIdentityCardNumber" : "291-82-9050",
        "nationalIdentificationNumber" : "",
        "passportNumber" : "LmPukdVmG"
    }
    ``` 