# Pulsar Wireshark dissector

The Pulsar Wireshark dissector allows to automatically decode the Pulsar binary protocol
and visualize useful debug information (linking requests with responses, latency stats, etc.)

### Install Wireshark

#### macOS

Recommend use this way to install Wireshark in macOS.

```bash
brew install homebrew/cask/wireshark
```

#### Ubuntu

```bash
sudo apt install wireshark
```

### Install dependencies

Install Wireshark with the development headers, necessary in order to build the plugin.

**NOTE**: Please make sure the Wireshark application version is the same as the Wireshark Headers version.

#### macOS

```shell
$ brew install wireshark
```

#### Ubuntu

```shell
$ sudo apt install wireshark-dev
```

### Compile the dissector

> If the compiler cannot find the wireshark headers,
> please use `-DWIRESHARK_INCLUDE_PATH=<WIRESHARK_INCLUDE_PATH>` to manually add include path.

Compile the dissector:

```shell
cd pulsar-client-cpp
cmake -DBUILD_WIRESHARK=ON .
make pulsar-dissector
```

This will create the `pulsar-dissector.so` plugin library in Wireshark directory.

### Install Wireshark dissector

Copy the dissector in the appropriate location so that Wireshark can find it at startup.

#### Find the Personal Plugins Location

1. Open Wireshark
2. Click About Wireshark
3. Find Folders tab

You can see the Personal Plugins Location, which is important for the next step.

#### Copy Wireshark dissector to appropriate location

Example for Wireshark 3.6.0 on macOS (This Location should be the previous step got Personal Plugins Location):

~/.local/lib/wireshark/plugins/3-6/

```shell
cd ~/.local/lib/wireshark/plugins/3-6/
mkdir -p ~/.local/lib/wireshark/plugins/3-6/epan
cd pulsar-client-cpp/wireshark
cp pulsar-dissector.so ~/.local/lib/wireshark/plugins/3-6/epan
```

#### Complete installation

Reboot Wireshark, you should be able to see the pulsar-dissector in:

View > Internals > Dissector Tables
