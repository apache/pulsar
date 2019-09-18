---
id: io-kinesis-source
title: Kinesis source connector
sidebar_label: Kinesis source connector
---

The Kinesis source connector pulls data from Amazon Kinesis and persists data into Pulsar.

## Configuration

The configuration of the Kinesis source connector has the following parameters.

### Parameter

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
`initialPositionInStream`|InitialPositionInStream|false|LATEST|The position where the connector starts from.<br/><br/>Below are the available options:<br/><br/><li>`AT_TIMESTAMP`: start from the record at or after the specified timestamp.<br/><br/><li>`LATEST`: start after the most recent data record.<br/><br/><li>`TRIM_HORIZON`: start from the oldest available data record.
`startAtTime`|Date|false|" " (empty string)|If set to `AT_TIMESTAMP`, it specifies the point in time to start consumption.
`applicationName`|String|false|Pulsar IO connector|The name of the Amazon Kinesis application. <br/><br/>By default, the application name is included in the user agent string used to make AWS requests. This can assist with troubleshooting, for example, distinguish requests made by separate connector instances.
`checkpointInterval`|long|false|60000|The frequency of the Kinesis stream checkpoint in milliseconds.
`backoffTime`|long|false|3000|The amount of time to delay between requests when the connector encounters a throttling exception from AWS Kinesis in milliseconds.
`numRetries`|int|false|3|The number of re-attempts when the connector encounters an exception while trying to set a checkpoint.
`receiveQueueSize`|int|false|1000|The maximum number of AWS records that can be buffered inside the connector. <br/><br/>Once the `receiveQueueSize` is reached, the connector does not consume any messages from Kinesis until some messages in the queue are successfully consumed.

### Example

Before using the Kinesis source connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "awsEndpoint": "https://some.endpoint.aws",
        "awsRegion": "us-east-1",
        "awsKinesisStreamName": "my-stream",
        "awsCredentialPluginParam": "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}",
        "applicationName": "My test application",
        "checkpointInterval": "30000",
        "backoffTime": "4000",
        "numRetries": "3",
        "receiveQueueSize": 2000,
        "initialPositionInStream": "TRIM_HORIZON",
        "startAtTime": "2019-03-05T19:28:58.000Z"
    }
    ```

* YAML

    ```yaml
    configs:
        awsEndpoint: "https://some.endpoint.aws"
        awsRegion: "us-east-1"
        awsKinesisStreamName: "my-stream"
        awsCredentialPluginParam: "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}"
        applicationName: "My test application"
        checkpointInterval: "30000"
        backoffTime: "4000"
        numRetries: "3"
        receiveQueueSize: 2000
        initialPositionInStream: "TRIM_HORIZON"
        startAtTime: "2019-03-05T19:28:58.000Z"
    ```