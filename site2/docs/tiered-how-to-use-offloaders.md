---
id: tiered-how-to-use-offloaders
title: How to use offloaders
sidebar_label: How to use offloaders
---

## Configure offload to auto run

You can configure namespace policies to offload data automatically once a threshold is reached. The threshold is based on the size of data that the topic has stored on the Pulsar cluster. Once the topic reaches the threshold, an offload operation will be triggered. Setting the threshold to a negative value will disable automatic offload. Setting the threshold to 0 will cause the broker to offload data as soon as it possibly can.

```bash
$ bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

> Automatic offload runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offload will not be triggered until the current segment is full.


## Trigger offload manually

Offload can be manually triggered via a REST endpoint on the Pulsar broker. We provide a CLI to call this REST endpoint.

When triggering offload, you must specify the maximum size of backlog which will be retained locally on the BookKeeper. The offload mechanism will not offload segments from the start of the topic backlog until this condition is met.

```bash
$ bin/pulsar-admin topics offload --size-threshold 10M my-tenant/my-namespace/topic1
Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
```

The command to trigger an offload will not wait until the offload operation has been completed. To check the status of the offload, use offload-status.

```bash
$ bin/pulsar-admin topics offload-status my-tenant/my-namespace/topic1
Offload is currently running
```

To wait for completing offload, add the -w flag.

```bash
$ bin/pulsar-admin topics offload-status -w my-tenant/my-namespace/topic1
Offload was a success
```

If there is an error offload, the error will be sent to the offload-status command.

```bash
$ bin/pulsar-admin topics offload-status persistent://public/default/topic1
Error in offload
null

Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=
````

