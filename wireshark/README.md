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

# How to use 

## Step 1: prepare PulsarApi.proto file
You need to put PulsarApi.proto to a separate path.

1. Open your Wireshark.

2. Go to **Edit > Preferences > Protocols > ProtoBuf > Protobuf**, and then search paths.

3. Add the path of PulsarApi.proto.

## Step 2: add pulsar.lua to plugins

1. Open Wireshark.

2. Go to **About Wireshark > Folders > Personal Lua Plugins > Plugin Path**.

3. Add pulsar.lua to this path.

## Step 3: start to use

This plugin registers a Pulsar protocol automatically in 6650. You can use it to decode Pulsar messages now.


