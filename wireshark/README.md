# how to use 

## prepare PulsarApi.proto file
You need to put PulsarApi.proto to a separate path.

1. Open your Wireshark.

2. Go to **Edit > Preferences > Protocols > ProtoBuf > Protobuf**, and then search paths.

3. Add the path of PulsarApi.proto.

## add pulsar.lua to plugins

open wireshark, go to 'About Wireshark', click  'Folders', then click 'Personal Lua Plugins'
go to the plugin path, add put pulsar.lua to this path.

## start to use

this plugin will register a pulsar protocol automatically in 6650, you can use it to decode pulsar message now.


