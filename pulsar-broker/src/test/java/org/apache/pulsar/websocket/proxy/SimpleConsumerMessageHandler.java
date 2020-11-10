package org.apache.pulsar.websocket.proxy;

import com.google.gson.JsonObject;

public interface SimpleConsumerMessageHandler {
  String handle(String id, JsonObject message);
}
