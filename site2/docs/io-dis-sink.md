---
id: io-DIS-sink
title: DIS sink connector
sidebar_label: DIS sink connector
---

The DIS sink connector pulls messages from Pulsar topics
and persists the messages to a DIS stream.



## Configuration

The configuration of the DIS sink connector has the following properties.



### Property

| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `region` |String|true|" " (empty string) | Information about the region where the DIS is located. |
| `disEndpoint` |String|true|" " (empty string) | Domain name or IP address of the server bearing the DIS REST service. |
| `projectId` | String|true|" " (empty string) | User Project ID. |
| `ak` |String| true|" " (empty string) | User AK. |
| `sk` | String|true|" " (empty string) | User SK. |
| `streamId` | String|true|" " (empty string) | Id of the DIS stream. |
| `streamName` |String| true|" " (empty string) | Name of the DIS stream. |


### Example

Before using the DIS sink connector, you need to create a configuration file through one of the following methods.

* JSON

    ```json
    {
        "region": "cn-north-4",
        "disEndpoint": "https://dis.cn-north-4.myhuaweicloud.com",
        "projectId": "fakeProjectId",
        "ak": "fakeSK",
        "sk": "fakeSK",
        "streamId": "fakeStreamId",
        "streamName": "fakeStreamName"
    }
    ```

* YAML

    ```yaml
    {
        region: cn-north-4
        disEndpoint: https://dis.cn-north-4.myhuaweicloud.com
        projectId: fakeProjectId
        ak: fakeAK
        sk: fakeSK
        streamId: fakeStreamId
        streamName: fakeStreamName
    }
    ```

