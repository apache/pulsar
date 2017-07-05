{% capture release_url %}https://github.com/yahoo/pulsar/releases/download/v{{ site.current_version }}/pulsar-{{ site.current_version }}-bin.tar.gz{% endcapture %}

## System requirements

Pulsar is currently available for **MacOS** and **Linux**. In order to use Pulsar, you'll need to install [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).

## Installing Pulsar

To get started running Pulsar, download a binary tarball release in one of the following ways:

* by clicking here:

  <a href="{{ release_url }}" class="download-btn btn btn-lg active" role="button" aria-pressed="true">Download Pulsar {{ site.current_version }}</a>

* from the Pulsar [releases page](https://github.com/yahoo/pulsar/releases/latest) (make sure to download the `pulsar-{{ site.latest }}-bin.tar.gz` release)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget {{ release_url }}
  ```

Once the tarball is downloaded, untar it and `cd` into the resulting directory:

```bash
$ tar xvf pulsar-{{ site.latest }}-bin.tar.gz
$ cd pulsar-{{ site.latest }}
```

## What your package contains

Directory | Contains
:---------|:--------
`bin` | Pulsar's [command-line tools](../../reference/CliTools), such as [`pulsar`](../../reference/CliTools#pulsar) and [`pulsar-admin`](../../reference/CliTools#pulsar-admin)
`conf` | Configuration files for Pulsar, including for [broker configuration](../../reference/Configuration#broker), [ZooKeeper configuration](../../reference/Configuration#zookeeper), and more
`data` | The data storage directory used by {% popover ZooKeeper %} and {% popover BookKeeper %}.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`logs` | Logs created by the installation.
