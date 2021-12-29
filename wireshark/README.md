# how to use 

## prepare PulsarApi.proto file
you need to put PulsarApi.proto to a separate path, open your wireshark,go to 
Edit->Preferences->Protocols->ProtoBuf->Protobuf search paths，add the path of PulsarApi.proto

## add pulsar.lua to plugins

open wireshark, go to 'About Wireshark', click  'Folders', then click 'Personal Lua Plugins'
go to the plugin path, add put pulsar.lua to this path.

## start to use

this plugin will register a pulsar protocol automatically in 6650, you can use it to decode pulsar message now.


