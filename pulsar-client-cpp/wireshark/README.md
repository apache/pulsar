<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar Wireshark dissector

The Pulsar Wireshark dissector allows to automatically decode the Pulsar binary protocol
and visualize useful debug information (linking requests with responses, latency stats, etc.)

## Install Wireshark

Based on your operating system, run the following command.

- macOS

```bash
brew install homebrew/cask/wireshark
```

- Ubuntu

```bash
sudo apt install wireshark
```

## Install dependencies

To build the Wireshark plugin, install Wireshark with the development headers

> **NOTE**
> 
> Make sure the Wireshark application version is the same as the Wireshark headers version.

- macOS

```shell
$ brew install wireshark
```

- Ubuntu

```shell
$ sudo apt install wireshark-dev
```

## Compile the dissector

> **Tip**
> 
> If the compiler cannot find the Wireshark headers, add the include path manually.
> `-DWIRESHARK_INCLUDE_PATH=<WIRESHARK_INCLUDE_PATH>`

Compile the dissector.

```shell
cd pulsar-client-cpp
cmake -DBUILD_WIRESHARK=ON .
make pulsar-dissector
```

This creates the `pulsar-dissector.so` plugin library in the Wireshark directory.

## Install Wireshark dissector

Copy the dissector in the appropriate location so that Wireshark can find it at startup.

### Find the Personal Plugins Location

1. Open Wireshark.
2. Click **About Wireshark**.
3. Click **Folders** tab.

You can see the location of personal plugins, which is important for the next step.

Example

Wireshark 3.6.0 on macOS

```shell
~/.local/lib/wireshark/plugins/3-6/
```

### Copy Wireshark dissector to appropriate location

```shell
mkdir -p ~/.local/lib/wireshark/plugins/3-6/epan
cd pulsar-client-cpp/wireshark
cp pulsar-dissector.so ~/.local/lib/wireshark/plugins/3-6/epan
```

### Complete installation

Reboot Wireshark. You can see the pulsar-dissector in **View > Internals > Dissector Tables**.
