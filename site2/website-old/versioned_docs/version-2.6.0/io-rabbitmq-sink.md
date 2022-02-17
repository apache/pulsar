---
id: version-2.6.0-io-rabbitmq-sink
title: RabbitMQ sink connector
sidebar_label: RabbitMQ sink connector
original_id: io-rabbitmq-sink
---

The RabbitMQ sink connector pulls messages from Pulsar topics 
and persist the messages to RabbitMQ queues.


## Configuration 

The configuration of the RabbitMQ sink connector has the following properties.


### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `connectionName` |String| true | " " (empty string) | The connection name. |
| `host` | String| true | " " (empty string) | The RabbitMQ host. |
| `port` | int |true | 5672 | The RabbitMQ port. |
| `virtualHost` |String|true | / | The virtual host used to connect to RabbitMQ. |
| `username` | String|false | guest | The username used to authenticate to RabbitMQ. |
| `password` | String|false | guest | The password used to authenticate to RabbitMQ. |
| `queueName` | String|true | " " (empty string) | The RabbitMQ queue name that messages should be read from or written to. |
| `requestedChannelMax` | int|false | 0 | The initially requested maximum channel number. <br><br>0 means unlimited. |
| `requestedFrameMax` | int|false |0 | The initially requested maximum frame size in octets. <br><br>0 means unlimited. |
| `connectionTimeout` | int|false | 60000 | The timeout of TCP connection establishment in milliseconds. <br><br>0 means infinite. |
| `handshakeTimeout` | int|false | 10000 | The timeout of AMQP0-9-1 protocol handshake in milliseconds. |
| `requestedHeartbeat` | int|false | 60 | The exchange to publish messages. |
| `exchangeName` | String|true | " " (empty string) | The maximum number of messages that the server delivers.<br><br> 0 means unlimited. |
| `prefetchGlobal` |String|true | " " (empty string) |The routing key used to publish messages. |


### Example

Before using the RabbitMQ sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "host": "localhost",
        "port": "5672",
        "virtualHost": "/",
        "username": "guest",
        "password": "guest",
        "queueName": "test-queue",
        "connectionName": "test-connection",
        "requestedChannelMax": "0",
        "requestedFrameMax": "0",
        "connectionTimeout": "60000",
        "handshakeTimeout": "10000",
        "requestedHeartbeat": "60",
        "exchangeName": "test-exchange",
        "routingKey": "test-key"
    }
    ```

* YAML

    ```yaml
    configs:
        host: "localhost"
        port: 5672
        virtualHost: "/",
        username: "guest"
        password: "guest"
        queueName: "test-queue"
        connectionName: "test-connection"
        requestedChannelMax: 0
        requestedFrameMax: 0
        connectionTimeout: 60000
        handshakeTimeout: 10000
        requestedHeartbeat: 60
        exchangeName: "test-exchange"
        routingKey: "test-key"
    ```

