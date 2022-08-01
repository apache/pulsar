---
id: io-hdfs
title: Hdfs Connector
sidebar_label: "Hdfs Connector"
original_id: io-hdfs
---

## Sink

The Hdfs Sink Connector is used to pull messages from Pulsar topics and persist the messages
to an hdfs file.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `hdfsConfigResources` | `null` | `true` | A file or comma separated list of files which contains the Hadoop file system configuration, e.g. 'core-site.xml', 'hdfs-site.xml'. |
| `directory` | `null` | `true` | The HDFS directory from which files should be read from or written to. |
| `encoding` | `null` | `false` | The character encoding for the files, e.g. UTF-8, ASCII, etc. |
| `compression` | `null` | `false` | The compression codec used to compress/de-compress the files on HDFS. |
| `kerberosUserPrincipal` | `null` | `false` | The Kerberos user principal account to use for authentication. |
| `keytab` | `null` | `false` | The full pathname to the Kerberos keytab file to use for authentication. |
| `filenamePrefix` | `null` | `false` | The prefix of the files to create inside the HDFS directory, i.e. a value of "topicA" will result in files named topicA-, topicA-, etc being produced. |
| `fileExtension` | `null` | `false` | The extension to add to the files written to HDFS, e.g. '.txt', '.seq', etc. |
| `separator` | `null` | `false` | The character to use to separate records in a text file. If no value is provided then the content from all of the records will be concatenated together in one continuous byte array. |
| `syncInterval` | `null` | `false` | The interval (in milliseconds) between calls to flush data to HDFS disk. |
| `maxPendingRecords` | `Integer.MAX_VALUE` | `false` | The maximum number of records that we hold in memory before acking. Default is `Integer.MAX_VALUE`. Setting this value to one, results in every record being sent to disk before the record is acked, while setting it to a higher values allows us to buffer records before flushing them all to disk. |