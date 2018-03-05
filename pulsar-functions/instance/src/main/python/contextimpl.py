#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# -*- encoding: utf-8 -*-

"""contextimpl.py: ContextImpl class that implements the Context interface
"""

import time
import os

from pulsarfunction import context
import util
import InstanceCommunication_pb2

# For keeping track of accumulated metrics
class AccumulatedMetricDatum(object):
  def __init__(self):
    self.count = 0.0
    self.sum = 0.0
    self.max = float('-inf')
    self.min = float('inf')

  def record(self, value):
    self.count += 1
    self.sum += value
    if value > self.max:
      self.max = value
    if value < self.min:
      self.min = value

class ContextImpl(context.Context):
  def __init__(self, instance_config, logger, pulsar_client, user_code, consumers):
    self.instance_config = instance_config
    self.log = logger
    self.pulsar_client = pulsar_client
    self.user_code_dir = os.path.dirname(user_code)
    self.consumers = consumers
    self.accumulated_metrics = {}
    self.publish_producers = {}
    self.publish_serializers = {}
    self.current_message_id = None
    self.current_topic_name = None
    self.current_start_time = None

  # Called on a per message basis to set the context for the current message
  def set_current_message_context(self, msgid, topic):
    self.current_message_id = msgid
    self.current_topic_name = topic
    self.current_start_time = time.time()

  def get_message_id(self):
    return self.current_message_id

  def get_topic_name(self):
    return self.current_topic_name

  def get_function_name(self):
    return self.instance_config.function_config.name

  def get_function_id(self):
    return self.instance_config.function_id

  def get_instance_id(self):
    return self.instance_config.instance_id

  def get_function_version(self):
    return self.instance_config.function_version

  def get_logger(self):
    return self.log

  def get_user_config_value(self, key):
    if key in self.instance_config.function_config.userConfig:
      return str(self.instance_config.function_config.userConfig[key])
    else:
      return None

  def record_metric(self, metric_name, metric_value):
    if not metric_name in self.accumulated_metrics:
      self.accumulated_metrics[metric_name] = AccumulatedMetricDatum()
    self.accumulated_metrics[metric_name].update(metric_value)

  def get_sink_topic(self):
    return self.instance_config.function_config.output

  def get_output_serde_class_name(self):
    return self.instance_config.function_config.outputSerdeClassName

  def publish(self, topic_name, message):
    return self.publish(topic_name, message, "serde.IdentitySerDe")

  def publish(self, topic_name, message, serde_class_name):
    if topic_name not in self.publish_producers:
      self.publish_producers[topic_name] = self.pulsar_client.create_producer(
        topic_name,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000
      )

    if serde_class_name not in self.publish_serializers:
      serde_klass = util.import_class(self.user_code_dir, serde_class_name)
      self.publish_serializers[serde_class_name] = serde_klass()

    output_bytes = self.publish_serializers[serde_class_name].serialize(message)
    self.publish_producers[topic_name].send_async(output_bytes, None)

  def ack(self, msgid, topic):
    if topic not in self.consumers:
      raise ValueError('Invalid topicname %s' % topic)
    self.consumers[topic].acknowledge(msgid)

  def get_and_reset_metrics(self):
    metrics = InstanceCommunication_pb2.MetricsData()
    for metric_name, accumulated_metric in self.accumulated_metrics.items():
      m = InstanceCommunication_pb2.MetricsData.DataDigest()
      m.count = accumulated_metric.count
      m.sum = accumulated_metric.sum
      m.max = accumulated_metric.max
      m.min = accumulated_metric.min
      metrics.metrics[metric_name] = m
    # TODO(sanjeev):- Make this thread safe
    self.accumulated_metrics.clear()
    return metrics
