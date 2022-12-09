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

> **Note**: This Lua script may cause a crash in the newest version of Wireshark, see [#18439](https://github.com/apache/pulsar/issues/18439). Make sure the version of Wireshark is earlier than v4.0.0.

# How to use 

## Step 1: prepare PulsarApi.proto file
You need to put PulsarApi.proto to a separate path.

1. Open your Wireshark.

2. Go to **Edit > Preferences > Protocols > ProtoBuf > Protobuf**, and then search paths.

3. Add the path of PulsarApi.proto.

4. Check `Dissect Protobuf fields as Wireshark fields` box. When this box is checked, 
you can use `pbf.pulsar.proto` to visit fields in protobuf package.  

## Step 2: add pulsar.lua to plugins

1. Open Wireshark.

2. Go to **About Wireshark > Folders > Personal Lua Plugins > Plugin Path**.

3. Add pulsar.lua to this path.

## Step 3: start to use

This plugin registers a Pulsar protocol automatically in 6650. You can use this Wireshark filter string to find out Pulsar packages (ignore ping/pong):

```
tcp.port eq 6650 and pulsar and pbf.pulsar.proto.BaseCommand.type ne "ping" and pbf.pulsar.proto.BaseCommand.type ne "pong"
```
