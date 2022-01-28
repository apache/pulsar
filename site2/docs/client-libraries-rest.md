---
id: client-libraries-rest
title: Pulsar REST
sidebar_label: REST
---

Pulsar not only provides REST endpoints to manage resources in Pulsar clusters, but also provides methods to query the state for those resources. In addition, Pulsar REST provides a simple way to interact with Pulsar **without using client libraries**, which is convenient for applications to use HTTP to interact with Pulsar. 

## Connection

To connect to Pulsar, you need to specify a URL.

- Produce messages to non-partitioned or partitioned topics

    ```
    brokerUrl:{8080/8081}/topics/{persistent/non-persistent}/{my-tenant}/{my-namespace}/{my-topic}
    ```

- Produce messages to specific partitions of partitioned topics

    ```
    brokerUrl:{8080/8081}/topics/{persistent/non-persistent}/{my-tenant}/{my-namespace}/{my-topic}/partitions/{partition-number}
    ```

## Producer

Currently, you can produce messages to the following destinations with tools like cURL or Postman via REST. 

- Non-partitioned or partitioned topics

- Specific partitions of partitioned topics

> **Note**
>
> You can only produce messages to **topics that already exist** in Pulsar via REST.

Consuming and reading messages via REST will be supported in the future.

### Message

- Below is the structure of a request payload.

    Parameter|Required?|Description
    |---|---|---
    `schemaVersion`|No| Schema version of existing schema used for this message </br></br>You need provide one of the following: <br/><br/> - `schemaVersion` <br/> - `keySchema`/`valueSchema`<br/><br/>If both of them are provided, then `schemaVersion` is used
    `keySchema/valueSchema`|No|Key schema / Value schema used for this message
    `producerName`|No|Producer name
    `Messages[] SingleMessage`|Yes|Messages to be sent

- Below is the structure of a message. 

    Parameter|Required?|Type|Description
    |---|---|---|---
    `payload`|Yes|`String`|Actual message payload </br></br>Messages are sent in strings and encoded with given schemas on the server side
    `properties`|No|`Map<String, String>`|Custom properties
    `key`|No|`String`|Partition key 
    `replicationClusters`|No|`List<String>`|Clusters to which messages replicate
    `eventTime`|No|`String`|Message event time
    `sequenceId`|No|`long`|Message sequence ID
    `disableReplication`|No|`boolean`|Whether to disable replication of messages
    `deliverAt`|No|`long`|Deliver messages only at or after specified absolute timestamp
    `deliverAfterMs`|No|`long`|Deliver messages only after specified relative delay (in milliseconds)

### Schema

- Currently, Primitive, Avro, JSON, and KeyValue schemas are supported.

- For Primitive, Avro and JSON schemas, schemas should be provided as the full schema encoded as a string.  

- If the schema is not set, messages are encoded with string schema.

### Example

Below is an example of sending messages to topics using JSON schema via REST.

Assume that you send messages representing the following class.

```java
   class Seller {
        public String state;
        public String street;
        public long zipCode;
    }

    class PC {
        public String brand;
        public String model;
        public int year;
        public GPU gpu;
        public Seller seller;
    }
```

Send messages to topics with JSON schema using the command below.

```shell
curl --location --request POST 'brokerUrl:{8080/8081}/topics/{persistent/non-persistent}/{my-tenant}/{my-namespace}/{my-topic}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "valueSchema": "{\"name\":\"\",\"schema\":\"eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IlBDIiwibmFtZXNwYWNlIjoib3JnLmFwYWNoZS5wdWxzYXIuYnJva2VyLmFkbWluLlRvcGljc1Rlc3QiLCJmaWVsZHMiOlt7Im5hbWUiOiJicmFuZCIsInR5cGUiOlsibnVsbCIsInN0cmluZyJdLCJkZWZhdWx0IjpudWxsfSx7Im5hbWUiOiJncHUiLCJ0eXBlIjpbIm51bGwiLHsidHlwZSI6ImVudW0iLCJuYW1lIjoiR1BVIiwic3ltYm9scyI6WyJBTUQiLCJOVklESUEiXX1dLCJkZWZhdWx0IjpudWxsfSx7Im5hbWUiOiJtb2RlbCIsInR5cGUiOlsibnVsbCIsInN0cmluZyJdLCJkZWZhdWx0IjpudWxsfSx7Im5hbWUiOiJzZWxsZXIiLCJ0eXBlIjpbIm51bGwiLHsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJTZWxsZXIiLCJmaWVsZHMiOlt7Im5hbWUiOiJzdGF0ZSIsInR5cGUiOlsibnVsbCIsInN0cmluZyJdLCJkZWZhdWx0IjpudWxsfSx7Im5hbWUiOiJzdHJlZXQiLCJ0eXBlIjpbIm51bGwiLCJzdHJpbmciXSwiZGVmYXVsdCI6bnVsbH0seyJuYW1lIjoiemlwQ29kZSIsInR5cGUiOiJsb25nIn1dfV0sImRlZmF1bHQiOm51bGx9LHsibmFtZSI6InllYXIiLCJ0eXBlIjoiaW50In1dfQ==\",\"type\":\"JSON\",\"properties\":{\"__jsr310ConversionEnabled\":\"false\",\"__alwaysAllowNull\":\"true\"},\"schemaDefinition\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"PC\\\",\\\"namespace\\\":\\\"org.apache.pulsar.broker.admin.TopicsTest\\\",\\\"fields\\\":[{\\\"name\\\":\\\"brand\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"],\\\"default\\\":null},{\\\"name\\\":\\\"gpu\\\",\\\"type\\\":[\\\"null\\\",{\\\"type\\\":\\\"enum\\\",\\\"name\\\":\\\"GPU\\\",\\\"symbols\\\":[\\\"AMD\\\",\\\"NVIDIA\\\"]}],\\\"default\\\":null},{\\\"name\\\":\\\"model\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"],\\\"default\\\":null},{\\\"name\\\":\\\"seller\\\",\\\"type\\\":[\\\"null\\\",{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Seller\\\",\\\"fields\\\":[{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"],\\\"default\\\":null},{\\\"name\\\":\\\"street\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"],\\\"default\\\":null},{\\\"name\\\":\\\"zipCode\\\",\\\"type\\\":\\\"long\\\"}]}],\\\"default\\\":null},{\\\"name\\\":\\\"year\\\",\\\"type\\\":\\\"int\\\"}]}\"}",

// Schema data is just the base 64 encoded schemaDefinition.

  "producerName": "rest-producer",
  "messages": [
    {
      "key":"my-key",
      "payload":"{\"brand\":\"dell\",\"model\":\"alienware\",\"year\":2021,\"gpu\":\"AMD\",\"seller\":{\"state\":\"WA\",\"street\":\"main street\",\"zipCode\":98004}}",
      "eventTime":1603045262772,
      "sequenceId":1
    },
    {
      "key":"my-key",
      "payload":"{\"brand\":\"asus\",\"model\":\"rog\",\"year\":2020,\"gpu\":\"NVIDIA\",\"seller\":{\"state\":\"CA\",\"street\":\"back street\",\"zipCode\":90232}}",
      "eventTime":1603045262772,
      "sequenceId":2
    }
  ]
}
`  
// Sample message
```
