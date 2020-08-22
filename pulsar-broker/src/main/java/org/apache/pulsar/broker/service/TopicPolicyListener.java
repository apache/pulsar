package org.apache.pulsar.broker.service;

public interface TopicPolicyListener<T> {
  void onUpdate(T data);
}
