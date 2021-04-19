---
id: version-2.7.2-io-hdfs2-sink
title: HDFS2 sink connector
sidebar_label: HDFS2 sink connector
original_id: io-hdfs2-sink
---

The HDFS2 sink connector pulls the messages from Pulsar topics 
and persists the messages to HDFS files.

## Configuration

The configuration of the HDFS2 sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `hdfsConfigResources` | String|true| None | A file or a comma-separated list containing the Hadoop file system configuration.<br/><br/>**Example**<br/>'core-site.xml'<br/>'hdfs-site.xml' |
| `directory` | String | true | None|The HDFS directory where files read from or written to. |
| `encoding` | String |false |None |The character encoding for the files.<br/><br/>**Example**<br/>UTF-8<br/>ASCII |
| `compression` | Compression |false |None |The compression code used to compress or de-compress the files on HDFS. <br/><br/>Below are the available options:<br/><li>BZIP2<br/><li>DEFLATE<br/><li>GZIP<br/><li>LZ4<br/><li>SNAPPY|
| `kerberosUserPrincipal` |String| false| None|The principal account of Kerberos user used for authentication. |
| `keytab` | String|false|None| The full pathname of the Kerberos keytab file used for authentication. |
| `filenamePrefix` |String| true, if `compression` is set to `None`. | None |The prefix of the files created inside the HDFS directory.<br/><br/>**Example**<br/> The value of topicA result in files named topicA-. |
| `fileExtension` | String| true | None | The extension added to the files written to HDFS.<br/><br/>**Example**<br/>'.txt'<br/> '.seq' |
| `separator` | char|false |None |The character used to separate records in a text file. <br/><br/>If no value is provided, the contents from all records are concatenated together in one continuous byte array. |
| `syncInterval` | long| false |0| The interval between calls to flush data to HDFS disk in milliseconds. |
| `maxPendingRecords` |int| false|Integer.MAX_VALUE |  The maximum number of records that hold in memory before acking. <br/><br/>Setting this property to 1 makes every record send to disk before the record is acked.<br/><br/>Setting this property to a higher value allows buffering records before flushing them to disk. 
| `subdirectoryPattern` | String | false | None | A subdirectory associated with the created time of the sink.<br/>The pattern is the formatted pattern of `directory`'s subdirectory.<br/><br/>See [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) for pattern's syntax. |

### Example

Before using the HDFS2 sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "hdfsConfigResources": "core-site.xml",
        "directory": "/foo/bar",
        "filenamePrefix": "prefix",
        "fileExtension": ".log",
        "compression": "SNAPPY",
        "subdirectoryPattern": "yyyy-MM-dd"
    }
    ```

* YAML

    ```yaml
    configs:
        hdfsConfigResources: "core-site.xml"
        directory: "/foo/bar"
        filenamePrefix: "prefix"
        fileExtension: ".log"
        compression: "SNAPPY"
        subdirectoryPattern: "yyyy-MM-dd"
    ```
