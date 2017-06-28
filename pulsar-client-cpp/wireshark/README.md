
# Pulsar Wireshark dissector

The Pulsar Wireshark dissector allows to automatically decode the Pulsar binary protocol
and visualize useful debug information (linking requests with responses, latency stats, etc..)

### Install dependencies

#### MacOS

Install Wireshark with the development headers, necessary in order to build the plugin.

```shell
$ brew install wireshark --with-headers
```

#### Ubuntu

```shell
$ apt-get install wireshark-dev
```

### Compile the dissector

Compile the dissector:

```shell
cd pulsar-client-cpp/wireshark
cmake .
make
```

This will create the `pulsar.so` plugin library.

Copy the plugin in the appropriate location so that Wireshark can find
it at startup:

```shell
$ cp pulsar.so ~/.config/wireshark/plugins
```
