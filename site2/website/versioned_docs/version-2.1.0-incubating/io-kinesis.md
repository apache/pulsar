---
id: version-2.1.0-incubating-io-kinesis
title: AWS Kinesis Connector
sidebar_label: AWS Kinesis Connector
original_id: io-kinesis
---

## Sink

The Kinesis Sink connector is used to pull data from Pulsar topics and persist the data into
AWS Kinesis.

### Sink Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| awsEndpoint | `true` | null | kinesis end-point url can be found at : https://docs.aws.amazon.com/general/latest/gr/rande.html |
| awsRegion | `true` | null | appropriate aws region eg: us-west-1, us-west-2 |
| awsKinesisStreamName | `true` | null | kinesis stream name |
| awsCredentialPluginName | `false` | null | Fully-Qualified class name of implementation of {@inject: github:`AwsCredentialProviderPlugin`:/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/AwsCredentialProviderPlugin.java}. It is a factory class which creates an AWSCredentialsProvider that will be used by Kinesis Sink. If it is empty then KinesisSink will create a default AWSCredentialsProvider which accepts json-map of credentials in `awsCredentialPluginParam` |
| awsCredentialPluginParam | `false` | null | json-parameters to initialize `AwsCredentialsProviderPlugin` |
| messageFormat | `true` | `ONLY_RAW_PAYLOAD` | Message format in which kinesis sink converts pulsar messages and publishes to kinesis streams |

### Message Formats

The available message formats are listed as below:

#### **ONLY_RAW_PAYLOAD**

Kinesis sink directly publishes pulsar message payload as a message into the configured kinesis stream.
#### **FULL_MESSAGE_IN_JSON**

Kinesis sink creates a json payload with pulsar message payload, properties and encryptionCtx, and publishes json payload into the configured kinesis stream.

#### **FULL_MESSAGE_IN_FB**

Kinesis sink creates a flatbuffer serialized paylaod with pulsar message payload, properties and encryptionCtx, and publishes flatbuffer payload into the configured kinesis stream.

