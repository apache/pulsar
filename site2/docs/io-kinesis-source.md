---
id: io-kinesis-source
title: Kinesis source connector
sidebar_label: Kinesis source connector
---

The Kinesis source connector pulls data from Amazon Kinesis and persists data into Pulsar.

Under the hood, it uses the [Kinesis Consumer Library](https://github.com/awslabs/amazon-kinesis-client) (KCL) to do the actual consuming of messages. The KCL uses dynamodb to track state for consumers.

## Configuration

The configuration of the Kinesis source connector has the following parameters.

### Parameter

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| applicationName | `false` | `pulsar-kinesis` | This is the application-name passed to the KCL and is used as the name to create dynamo tables. This should be overriden if you plan on having multiple instances of this source |
| awsEndpoint | `true` | null | kinesis end-point url can be found at : https://docs.aws.amazon.com/general/latest/gr/rande.html |
| awsRegion | `true` | null | appropriate aws region eg: us-west-1, us-west-2 |
| awsKinesisStreamName | `true` | null | kinesis stream name |
| awsCredentialPluginName | `false` | null | Fully-Qualified class name of implementation of {@inject: github:`AwsCredentialProviderPlugin`:/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/AwsCredentialProviderPlugin.java}. It is a factory class which creates an AWSCredentialsProvider that will be used by Kinesis Source. If it is empty then KinesisSource will create a default AWSCredentialsProvider which accepts json-map of credentials in `awsCredentialPluginParam` |
| awsCredentialPluginParam | `false` | null | json-parameters to initialize `AwsCredentialsProviderPlugin` |
| backoffTime | `false` | 3000 | The interval (in ms) of how long to wait for throttles or other errors |
| checkpointInterval | `false` | 60000 | The interval (in ms) of how often to checkpoint the position of consumers to dynamodb |
| cloudwatchEndpoint | `false` | `` | The endpoint to use for cloudwatch, defaults to regional endpoint if not provided |
| dynamoEndpoint | `false` | `` | The endpoint to use for dynamodb, defaults to regional endpoint if not provided |
| initialPositionInStream | `false` | `LATEST` | Where to start in the stream, valid values are `AT_TIMESTAMAP`, `LATEST`, and `TRIM_HORIZON` |
| numRetries | `false` | `3` | How many times to retries before failing the source |
| startAtTime | `false` | `` | When using `AT_TIMESTAMAP` for `initialPositionInStream`, this sets the timestamp to start at, must be a valid string representing a java date |
| useEnhancedFanOut | `false` | `true` | Defaults to using push-bashed enhanced fan out, set to false to fall back to polling, note that enhanced fan-out has an additional cost |

### Encrypted Messages

Currently, this source only supports raw messages. if you use KMS encrypted messages, the encrypted messages will be sent downstream. A future version of this connector should support being able to decrypt the messages.

### Built-in `AwsCredentialProviderPlugin` plugins

#### `org.apache.pulsar.io.kinesis.AwsDefaultProviderChainPlugin`
This plugin takes no configuration, it uses the default AWS provider chain. See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) for more details

#### `org.apache.pulsar.io.kinesis.STSAssumeRoleProviderPlugin`
This plugin takes a configuration (via the `awsCredentialPluginParam`) that describes a role to assume when running the KCL.

This configuration takes the form of a small json document like:
```Json
{"roleArn": "arn...", "roleSessionName": "name"}
```
