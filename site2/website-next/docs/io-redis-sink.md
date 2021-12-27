---
id: io-redis-sink
title: Redis sink connector
sidebar_label: "Redis sink connector"
---

The  Redis sink connector pulls messages from Pulsar topics 
and persists the messages to a Redis database.



## Configuration

The configuration of the Redis sink connector has the following properties.



### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `redisHosts` |String|true|" " (empty string) | A comma-separated list of Redis hosts to connect to. |
| `redisPassword` |String|false|" " (empty string) | The password used to connect to Redis. |
| `redisDatabase` | int|true|0  | The Redis database to connect to. |
| `clientMode` |String| false|Standalone | The client mode when interacting with Redis cluster. <br /><br />Below are the available options: <br /><li>Standalone<br /></li><li>Cluster </li>|
| `autoReconnect` | boolean|false|true | Whether the Redis client automatically reconnect or not. |
| `requestQueue` | int|false|2147483647 | The maximum number of queued requests to Redis. |
| `tcpNoDelay` |boolean| false| false | Whether to enable TCP with no delay or not. |
| `keepAlive` | boolean|false | false |Whether to enable a keepalive to Redis or not. |
| `connectTimeout` |long| false|10000 | The time to wait before timing out when connecting in milliseconds. |
| `operationTimeout` | long|false|10000 | The time before an operation is marked as timed out in milliseconds . |
| `batchTimeMs` | int|false|1000 | The Redis operation time in milliseconds. |
| `batchSize` | int|false|200 | The batch size of writing to Redis database. |


### Example

Before using the Redis sink connector, you need to create a configuration file in the path you will start Pulsar service (i.e. `PULSAR_HOME`) through one of the following methods.

* JSON

  ```json
  
  {
     "configs": {
        "redisHosts": "localhost:6379",
        "redisPassword": "mypassword",
        "redisDatabase": "0",
        "clientMode": "Standalone",
        "operationTimeout": "2000",
        "batchSize": "1",
        "batchTimeMs": "1000",
        "connectTimeout": "3000"
     }
  }
  
  ```

* YAML

  ```yaml
  
  configs:
      redisHosts: "localhost:6379"
      redisPassword: "mypassword"
      redisDatabase: 0
      clientMode: "Standalone"
      operationTimeout: 2000
      batchSize: 1
      batchTimeMs: 1000
      connectTimeout: 3000
  
  ```

### Usage

This example shows how to write records to a Redis database using the Pulsar Redis connector.

1. Start a Redis server.

   ```bash
   
   $ docker pull redis:5.0.5
   $ docker run -d -p 6379:6379 --name my-redis redis:5.0.5 --requirepass "mypassword"
   
   ```

2. Start a Pulsar service locally in standalone mode.

   ```bash
   
   $ bin/pulsar standalone
   
   ```

   Make sure the NAR file is available at `connectors/pulsar-io-redis-@pulsar:version@.nar`.
   
3. Start the Pulsar Redis connector in local run mode using one of the following methods.

   * Use the **JSON** configuration file as shown previously. 

       ```bash
       
       $ bin/pulsar-admin sinks localrun \
       --archive connectors/pulsar-io-redis-@pulsar:version@.nar \
       --tenant public \
       --namespace default \
       --name my-redis-sink \
       --sink-config '{"redisHosts": "localhost:6379","redisPassword": "mypassword","redisDatabase": "0","clientMode": "Standalone","operationTimeout": "3000","batchSize": "1"}' \
       --inputs my-redis-topic
       
       ```

   * Use the **YAML** configuration file as shown previously.

       ```bash
       
       $ bin/pulsar-admin sinks localrun \
       --archive connectors/pulsar-io-redis-@pulsar:version@.nar \
       --tenant public \
       --namespace default \
       --name my-redis-sink \
       --sink-config-file redis-sink-config.yaml \
       --inputs my-redis-topic
       
       ```

4. Publish records to the topic.

   ```bash
   
   $ bin/pulsar-client produce \
       persistent://public/default/my-redis-topic \
       -k "streaming" \
       -m "Pulsar"
   
   ```

5. Start a Redis client in Docker.

   ```bash
   
   $ docker exec -it my-redis redis-cli -a "mypassword"
   
   ```

6. Check the key/value in Redis.

   ```
   
   127.0.0.1:6379> keys *
   1) "streaming"
   127.0.0.1:6379> get "streaming"
   "Pulsar"
   
   ```

