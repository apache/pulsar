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


