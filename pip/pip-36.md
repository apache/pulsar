# PIP-36: Max Message Size

- Status: Proposed
- Author: Yong Zhang
- Discusstion Thread:
- Issue: 

## Motivation

Currently `MaxMessageSize` is hardcoded in Pulsar and it can’t be modified in server configuration. So there is no way when user want to modify the limit to transfer larger size message. 
Hence we add a `MaxMessageSize` config in `broker.conf` to solve this problem. Because broker server will decide how much message size will be received so client need know how much message client can be sent. 

Hence we propose adding a new flag `max_message_size` in protocol to tell a client what is the max message size that it can use once it connected.

## Changes
### Wire Protocol
We propose to introduce an optional field called `max_message_size` in `CommandConnected` .
```
message CommandConnected {
	required string server_version = 1;
	optional int32 protocol_version = 2 [default = 0];

	// used for telling clients what is the max message size it can use
	optional int64 max_message_size = 3 [default = 5 * 1024 * 1024];
}

```

On server side, we need send `max_message_size` to clients when broker complete connect:
```
// complete the connect and sent newConnected command
private void completeConnect(int clientProtoVersion, String clientVersion, long maxMessageSize) {
    ctx.writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize));
    state = State.Connected;
    remoteEndpointProtocolVersion = clientProtoVersion;
    if (isNotBlank(clientVersion) && !clientVersion.contains(" ") /* ignore default version: pulsar client */) {
        this.clientVersion = clientVersion.intern();
    }
}
```

On client side, client should set max message size in configuration and it should be smaller than server support.

### Implement
We defined three value about message size in `Commands`:

	- DEFAULT_MAX_MESSAGE_SIZE = 5 * 1024 * 1024
	- MESSAGE_SIZE_FRAME_PADDING = 10 * 1024
	- INVALID_MAX_MESSAGE_SIZE = -1

> **DEFAULT_MAX_MESSAGE_SIZE** is used to set where is not specify the max message size. And **MESSAGE_SIZE_FRAME_PADDING** is the message meta info size. Sometimes the message size is not necessary in *Connected* so you can choose **INVALID_MAX_MESSAGE_SIZE** and it will not in *Connected*  command. 

**Broker**
 we need send `max_message_size` to clients when broker complete connect:
```
// complete the connect and sent newConnected command
private void completeConnect(int clientProtoVersion, String clientVersion, long maxMessageSize) {
 ctx.writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize));
 state = State.Connected;
 remoteEndpointProtocolVersion = clientProtoVersion;
 if (isNotBlank(clientVersion) && !clientVersion.contains(" ") /* ignore default version: pulsar client */) {
 this.clientVersion = clientVersion.intern();
 }
}
```

**Client**
Client shouldn’t set max message size in configuration and When client connect server it will get the max message size how broker can received. And it replaces the `LengthFieldBasedFrameDecoder` once it get the new message size. When client sends messages it will compare with the message size, it will throw an exception once the message size exceed.

**Proxy**
It will have two operation when client connect to proxy. One is client is looking up brokers and another is client has know which broker to connect and it connecting to broker. There is no messages except connect message is transferring between proxy and client when client just looking up brokers. So it needn’t set message size when proxy connected. But when client has know which broker to connect proxy server will create a direct proxy, then proxy and client will receive the message max size and replace the `LengthFieldBasedFrameDecoder` between proxy and client.
