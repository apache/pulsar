---
id: tiered-storage-filesystem
title: Use filesystem offloader with Pulsar
sidebar_label: "Filesystem offloader"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


This chapter guides you through every step of installing and configuring the filesystem offloader and using it with Pulsar.

## Installation

This section describes how to install the filesystem offloader.

### Prerequisite

- Pulsar: 2.4.2 or higher versions

### Steps

1. [Download the Pulsar tarball](getting-started-standalone.md#download-pulsar-distribution).
2. Download and untar the Pulsar offloaders package, then copy the Pulsar offloaders as `offloaders` in the Pulsar directory. See [Install tiered storage offloaders](tiered-storage-overview.md#how-to-install-tiered-storage-offloaders).

## Configuration

:::note

Before offloading data from BookKeeper to filesystem, you need to configure some properties of the filesystem offloader driver.

:::

Besides, you can also configure the filesystem offloader to run it automatically or trigger it manually.

### Configure filesystem offloader driver

You can configure the filesystem offloader driver in the `broker.conf` or `standalone.conf` configuration file.

````mdx-code-block
<Tabs
  defaultValue="HDFS"
  values={[{"label":"HDFS","value":"HDFS"},{"label":"NFS","value":"NFS"}]}>
<TabItem value="HDFS">

- **Required** configurations are as below.

  Parameter | Description | Example value
  |---|---|---
  `managedLedgerOffloadDriver` | Offloader driver name, which is case-insensitive. | filesystem
  `fileSystemURI` | Connection address, which is the URI to access the default Hadoop distributed file system. | hdfs://127.0.0.1:9000
  `offloadersDirectory` | Offloader directory | offloaders
  `fileSystemProfilePath` | Hadoop profile path. The configuration file is stored in the Hadoop profile path. It contains various settings for Hadoop performance tuning. | conf/filesystem_offload_core_site.xml


- **Optional** configurations are as below.

  Parameter| Description | Example value
  |---|---|---
  `managedLedgerMinLedgerRolloverTimeMinutes`|Minimum time between ledger rollover for a topic. <br /><br />**Note**: it is not recommended to set this parameter in the production environment.|2
  `managedLedgerMaxEntriesPerLedger`|Maximum number of entries to append to a ledger before triggering a rollover.<br /><br />**Note**: it is not recommended to set this parameter in the production environment.|5000

</TabItem>
<TabItem value="NFS">

- **Required** configurations are as below.
  Parameter | Description | Example value
  |---|---|---
  `managedLedgerOffloadDriver` | Offloader driver name, which is case-insensitive. | filesystem
  `offloadersDirectory` | Offloader directory | offloaders
  `fileSystemProfilePath` | NFS profile path. The configuration file is stored in the NFS profile path. It contains various settings for performance tuning. | conf/filesystem_offload_core_site.xml

- **Optional** configurations are as below.

  Parameter| Description | Example value
  |---|---|---
  `managedLedgerMinLedgerRolloverTimeMinutes`|Minimum time between ledger rollover for a topic. <br /><br />**Note**: it is not recommended to set this parameter in the production environment.|2
  `managedLedgerMaxEntriesPerLedger`|Maximum number of entries to append to a ledger before triggering a rollover.<br /><br />**Note**: it is not recommended to set this parameter in the production environment.|5000

</TabItem>

</Tabs>
````

### Run filesystem offloader automatically

You can configure the namespace policy to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored on a Pulsar cluster. Once the topic storage reaches the threshold, an offload operation is triggered automatically.

Threshold value|Action
|---|---
| > 0 | It triggers the offloading operation if the topic storage reaches its threshold.
= 0|It causes a broker to offload data as soon as possible.
< 0 |It disables automatic offloading operation.

Automatic offload runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, the filesystem offloader does not work until the current segment is full.

You can configure the threshold using CLI tools, such as pulsar-admin.

#### Example

This example sets the filesystem offloader threshold to 10 MB using pulsar-admin.

```bash
pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

:::tip

For more information about the `pulsar-admin namespaces set-offload-threshold options` command, including flags, descriptions, default values, and shorthands, see [here](/tools/pulsar-admin/).

:::

### Run filesystem offloader manually

For individual topics, you can trigger the filesystem offloader manually using one of the following methods:

- Use the REST endpoint.

- Use CLI tools (such as pulsar-admin).

To manually trigger the filesystem offloader via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained on a Pulsar cluster for a topic. If the size of the topic data on the Pulsar cluster exceeds this threshold, segments from the topic are offloaded to the filesystem until the threshold is no longer exceeded. Older segments are offloaded first.

#### Example

- This example manually runs the filesystem offloader using pulsar-admin.

  ```bash
  pulsar-admin topics offload --size-threshold 10M persistent://my-tenant/my-namespace/topic1
  ```

  **Output**

  ```bash
  Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
  ```

  :::tip

  For more information about the `pulsar-admin topics offload options` command, including flags, descriptions, default values, and shorthands, see [here](/tools/pulsar-admin/).

  :::

- This example checks filesystem offloader status using pulsar-admin.

  ```bash
  pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
  ```

  **Output**

  ```bash
  Offload is currently running
  ```

  To wait for the filesystem to complete the job, add the `-w` flag.

  ```bash
  pulsar-admin topics offload-status -w persistent://my-tenant/my-namespace/topic1
  ```

  **Output**

  ```
  Offload was a success
  ```

  If there is an error in the offloading operation, the error is propagated to the `pulsar-admin topics offload-status` command.

  ```bash
  pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
  ```

  **Output**

  ```
  Error in offload
  null

  Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=
  ```

  :::tip

  For more information about the `pulsar-admin topics offload-status options` command, including flags, descriptions, default values, and shorthands, see [here](/tools/pulsar-admin/).

  :::

## Tutorial

This section provides step-by-step instructions on how to use the filesystem offloader to move data from Pulsar to Hadoop Distributed File System (HDFS) or Network File system (NFS).

### Offload data to HDFS

:::tip

This tutorial sets up a Hadoop single node cluster and uses Hadoop 3.2.1. For details about how to set up a Hadoop single node cluster, see [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

:::

#### Step 1: Prepare the HDFS environment

1. Download and uncompress Hadoop 3.2.1.

   ```shell
   wget https://mirrors.bfsu.edu.cn/apache/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz

   tar -zxvf hadoop-3.2.1.tar.gz -C $HADOOP_HOME
   ```

2. Configure Hadoop.

   ```xml
   # $HADOOP_HOME/etc/hadoop/core-site.xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://localhost:9000</value>
       </property>
   </configuration>

   # $HADOOP_HOME/etc/hadoop/hdfs-site.xml
   <configuration>
       <property>
           <name>dfs.replication</name>
           <value>1</value>
       </property>
   </configuration>
   ```

3. Set passphraseless ssh.

   ```bash
   # Now check that you can ssh to the localhost without a passphrase:
   ssh localhost
   # If you cannot ssh to localhost without a passphrase, execute the following commands
   ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 0600 ~/.ssh/authorized_keys
   ```

4. Start HDFS.

   ```bash
   # don't execute this command repeatedly, repeat execute will cauld the clusterId of the datanode is not consistent with namenode
   $HADOOP_HOME/bin/hadoop namenode -format
   $HADOOP_HOME/sbin/start-dfs.sh
   ```

5. Navigate to the [HDFS website](http://localhost:9870/).

   You can see the **Overview** page.

   ![](/assets/FileSystem-1.png)

   1. At the top navigation bar, click **Datanodes** to check DataNode information.

       ![](/assets/FileSystem-2.png)

   2. Click **HTTP Address** to get more detailed information about localhost:9866.

       As can be seen below, the size of **Capacity Used** is 4 KB, which is the initial value.

       ![](/assets/FileSystem-3.png)

#### Step 2: Install the filesystem offloader

For details, see [installation](#installation).

#### Step 3: Configure the filesystem offloader

As indicated in the [configuration](#configuration) section, you need to configure some properties for the filesystem offloader driver before using it. This tutorial assumes that you have configured the filesystem offloader driver as below and run Pulsar in **standalone** mode.

Set the following configurations in the `conf/standalone.conf` file.

```conf
managedLedgerOffloadDriver=filesystem
fileSystemURI=hdfs://127.0.0.1:9000
fileSystemProfilePath=conf/filesystem_offload_core_site.xml
```

:::note

For testing purposes, you can set the following two configurations to speed up ledger rollover, but it is not recommended that you set them in the production environment.

:::

```conf
managedLedgerMinLedgerRolloverTimeMinutes=1
managedLedgerMaxEntriesPerLedger=100
```

#### Step 4: Offload data from BookKeeper to filesystem

Execute the following commands in the repository where you download Pulsar tarball. For example, `~/path/to/apache-pulsar-2.5.1`.

1. Start Pulsar standalone.

   ```shell
   bin/pulsar standalone -a 127.0.0.1
   ```

2. To ensure the data generated is not deleted immediately, it is recommended to set the [retention policy](cookbooks-retention-expiry.md#retention-policies), which can be either a **size** limit or a **time** limit. The larger value you set for the retention policy, the longer the data can be retained.

   ```shell
   bin/pulsar-admin namespaces set-retention public/default --size 100M --time 2d
   ```

   :::tip

   For more information about the `pulsarctl namespaces set-retention options` command, including flags, descriptions, default values, and shorthands, see [here](https://docs.streamnative.io/pulsarctl/v2.7.0.6/#-em-set-retention-em-).

   :::

3. Produce data using pulsar-client.

   ```shell
   bin/pulsar-client produce -m "Hello FileSystem Offloader" -n 1000 public/default/fs-test
   ```

4. The offloading operation starts after a ledger rollover is triggered. To ensure offload data successfully, it is recommended that you wait until several ledger rollovers are triggered. In this case, you might need to wait for a second. You can check the ledger status using pulsarctl.

   ```shell
   bin/pulsar-admin topics stats-internal public/default/fs-test
   ```

   **Output**

   The data of the ledger 696 is not offloaded.

   ```shell
   {
   "version": 1,
   "creationDate": "2020-06-16T21:46:25.807+08:00",
   "modificationDate": "2020-06-16T21:46:25.821+08:00",
   "ledgers": [
   {
       "ledgerId": 696,
       "isOffloaded": false
   }
   ],
   "cursors": {}
   }
   ```

5. Wait for a second and send more messages to the topic.

   ```shell
   bin/pulsar-client produce -m "Hello FileSystem Offloader" -n 1000 public/default/fs-test
   ```

6. Check the ledger status using pulsarctl.

   ```shell
   bin/pulsar-admin topics stats-internal public/default/fs-test
   ```

   **Output**

   The ledger 696 is rolled over.

   ```shell
   {
   "version": 2,
   "creationDate": "2020-06-16T21:46:25.807+08:00",
   "modificationDate": "2020-06-16T21:48:52.288+08:00",
   "ledgers": [
   {
       "ledgerId": 696,
       "entries": 1001,
       "size": 81695,
       "isOffloaded": false
   },
   {
       "ledgerId": 697,
       "isOffloaded": false
   }
   ],
   "cursors": {}
   }
   ```

7. Trigger the offloading operation manually using pulsarctl.

   ```shell
   bin/pulsar-admin topics offload -s 0 public/default/fs-test
   ```

   **Output**

   Data in ledgers before the ledger 697 is offloaded.

   ```shell
   # offload info, the ledgers before 697 will be offloaded
   Offload triggered for persistent://public/default/fs-test3 for messages before 697:0:-1
   ```

8.  Check the ledger status using pulsarctl.

   ```shell
   bin/pulsar-admin topics stats-internal public/default/fs-test
   ```

   **Output**

   The data of the ledger 696 is offloaded.

   ```shell
   {
   "version": 4,
   "creationDate": "2020-06-16T21:46:25.807+08:00",
   "modificationDate": "2020-06-16T21:52:13.25+08:00",
   "ledgers": [
   {
       "ledgerId": 696,
       "entries": 1001,
       "size": 81695,
       "isOffloaded": true
   },
   {
       "ledgerId": 697,
       "isOffloaded": false
   }
   ],
   "cursors": {}
   }
   ```

   And the **Capacity Used** is changed from 4 KB to 116.46 KB.

   ![](/assets/FileSystem-8.png)


### Offload data to NFS

:::note

In this section, it is assumed that you have enabled NFS service and set the shared path of your NFS service. In this section, `/Users/test` is used as the shared path of NFS service.

:::

#### Step 1: Install the filesystem offloader

For details, see [installation](#installation).

#### Step 2: Mount your NFS to your local filesystem

This example mounts */Users/pulsar_nfs* to */Users/test*.

```shell
mount -e 192.168.0.103:/Users/test/Users/pulsar_nfs
```

#### Step 3: Configure the filesystem offloader driver

As indicated in the [configuration](#configuration) section, you need to configure some properties for the filesystem offloader driver before using it. This tutorial assumes that you have configured the filesystem offloader driver as below and run Pulsar in **standalone** mode.

1. Set the following configurations in the `conf/standalone.conf` file.

   ```conf
   managedLedgerOffloadDriver=filesystem
   fileSystemProfilePath=conf/filesystem_offload_core_site.xml
   ```

2. Modify the *filesystem_offload_core_site.xml* as follows.

   ```xml
   <property>
       <name>fs.defaultFS</name>
       <value>file:///</value>
   </property>

   <property>
       <name>hadoop.tmp.dir</name>
       <value>file:///Users/pulsar_nfs</value>
   </property>

   <property>
       <name>io.file.buffer.size</name>
       <value>4096</value>
   </property>

   <property>
       <name>io.seqfile.compress.blocksize</name>
       <value>1000000</value>
   </property>
   <property>

       <name>io.seqfile.compression.type</name>
       <value>BLOCK</value>
   </property>

   <property>
       <name>io.map.index.interval</name>
       <value>128</value>
   </property>
   ```

#### Step 4: Offload data from BookKeeper to filesystem

Refer to the step 4 of [Offload data to HDFS](#step-4-offload-data-from-bookkeeper-to-filesystem).


## Read offloaded data from filesystem

* The offloaded data is stored as `MapFile` in the following new path of the filesystem:

  ```properties
  path = storageBasePath + "/" + managedLedgerName + "/" + ledgerId + "-" + uuid.toString();
  ```

  * `storageBasePath` is the value of `hadoop.tmp.dir`, which is configured in `broker.conf` or `filesystem_offload_core_site.xml`.
  * `managedLedgerName` is the ledger name of the persistentTopic manager.

  ```shell
  managedLedgerName of persistent://public/default/topics-name is public/default/persistent/topics-name.
  ```

  You can use the following method to get `managedLedgerName`:

  ```shell
  String managedLedgerName = TopicName.get("persistent://public/default/topics-name").getPersistenceNamingEncoding();
  ```

To read data out as ledger entries from the filesystem, complete the following steps.
1. Create a reader to read both `MapFile` with a new path and the `configuration` of the filesystem.

  ```shell
  MapFile.Reader reader = new MapFile.Reader(new Path(dataFilePath),  configuration);
  ```

2. Read the data as `LedgerEntry` from the filesystem.

  ```java
    LongWritable key = new LongWritable();
    BytesWritable value = new BytesWritable();
    key.set(nextExpectedId - 1);
    reader.seek(key);
    reader.next(key, value);
    int length = value.getLength();
    long entryId = key.get();
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(length, length);
    buf.writeBytes(value.copyBytes());
    LedgerEntryImpl ledgerEntry = LedgerEntryImpl.create(ledgerId, entryId, length, buf);
  ```

3. Deserialize the `LedgerEntry` to `Message`.

  ```java
       ByteBuf metadataAndPayload = ledgerEntry.getDataBuffer();
       long totalSize = metadataAndPayload.readableBytes();
       BrokerEntryMetadata brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(metadataAndPayload);
       MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);

       Map<String, String> properties = new TreeMap();
       properties.put("X-Pulsar-batch-size", String.valueOf(totalSize
               - metadata.getSerializedSize()));
       properties.put("TOTAL-CHUNKS", Integer.toString(metadata.getNumChunksFromMsg()));
       properties.put("CHUNK-ID", Integer.toString(metadata.getChunkId()));

       // Decode if needed
       CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
       ByteBuf uncompressedPayload = codec.decode(metadataAndPayload, metadata.getUncompressedSize());
       // Copy into a heap buffer for output stream compatibility
       ByteBuf data = PulsarByteBufAllocator.DEFAULT.heapBuffer(uncompressedPayload.readableBytes(),
               uncompressedPayload.readableBytes());
       data.writeBytes(uncompressedPayload);
       uncompressedPayload.release();

       MessageImpl message = new MessageImpl(topic, ((PositionImpl)ledgerEntry.getPosition()).toString(), properties,
               data, Schema.BYTES, metadata);
       message.setBrokerEntryMetadata(brokerEntryMetadata);
  ```

