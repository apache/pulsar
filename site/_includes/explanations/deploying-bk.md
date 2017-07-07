{% popover BookKeeper %} provides persistent message storage for Pulsar.

Each Pulsar {% popover cluster %} needs to have its own cluster of {% popover bookies %}. The BookKeeper cluster shares a local ZooKeeper quorum with the Pulsar cluster.

### Configuring bookies

BookKeeper bookies can be configured using the [`conf/bookkeeper.conf`](../../reference/Configuration#bookkeeper) configuration file. The most important aspect of configuring each bookie is ensuring that the [`zkServers`](../../reference/Configuration#bookkeeper-zkServers) parameter is set to the connection string for the Pulsar cluster's local ZooKeeper.

### Starting up bookies

You can start up a bookie in two ways: in the foreground or as a background daemon.

To start up a bookie in the foreground, use the [`bookeeper`](../../reference/CliTools#bookkeeper)

```shell
$ bin/pulsar-daemon start bookie
```

You can verify that the bookie is working properly using the `bookiesanity` command for the [BookKeeper shell](../../reference/CliTools#bookkeeper-shell):

```shell
$ bin/bookkeeper shell bookiesanity
```

This will create a new ledger on the local bookie, write a few entries, read them back and finally delete the ledger.

### Hardware considerations

{% popover Bookie %} hosts are responsible for storing message data on disk. In order for bookies to provide optimal performance, it's essential that they have a suitable hardware configuration. There are two key dimensions to bookie hardware capacity:

* Disk I/O capacity read/write
* Storage capacity

Message entries written to bookies are always synced to disk before returning an {% popover acknowledgement %} to the Pulsar {% popover broker %}. To ensure low write latency, BookKeeper is
designed to use multiple devices:

* A **journal** to ensure durability. For sequential writes, it's critical to have fast [fsync](https://linux.die.net/man/2/fsync) operations on bookie hosts. Typically, small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) should suffice, or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive) (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID)s controller and a battery-backed write cache. Both solutions can reach fsync latency of ~0.4 ms.
* A **ledger storage device** is where data is stored until all {% popover consumers %} have {% popover acknowledged %} the message. Writes will happen in the background, so write I/O is not a big concern. Reads will happen sequentially most of the time and the backlog is drained only in case of consumer drain. To store large amounts of data, a typical configuration will involve multiple HDDs with a RAID controller.
