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
import json

import pulsar
import util
import InstanceCommunication_pb2

from prometheus_client import Gauge
from function_stats import Stats

# For keeping track of accumulated metrics
class AccumulatedMetricDatum(object):
  def __init__(self):
    self.count = 0.0
    self.sum = 0.0
    self.max = float('-inf')
    self.min = float('inf')

  def update(self, value):
    self.count += 1
    self.sum += value
    if value > self.max:
      self.max = value
    if value < self.min:
      self.min = value

class ContextImpl(pulsar.Context):

  # add label to indicate user metric
  user_metrics_label_names = Stats.metrics_label_names + ["metric"]

  def __init__(self, instance_config, logger, pulsar_client, user_code, consumers, secrets_provider, metrics_labels):
    self.instance_config = instance_config
    self.log = logger
    self.pulsar_client = pulsar_client
    self.user_code_dir = os.path.dirname(user_code)
    self.consumers = consumers
    self.secrets_provider = secrets_provider
    self.accumulated_metrics = {}
    self.publish_producers = {}
    self.publish_serializers = {}
    self.current_message_id = None
    self.current_input_topic_name = None
    self.current_start_time = None
    self.user_config = json.loads(instance_config.function_details.userConfig) \
      if instance_config.function_details.userConfig \
      else []
    self.secrets_map = json.loads(instance_config.function_details.secretsMap) \
      if instance_config.function_details.secretsMap \
      else {}

    self.metrics_labels = metrics_labels
    self.user_metrics_labels = dict()
    self.user_metrics_gauge = Gauge("pulsar_function_user_metric",
                                    'Pulsar Function user defined metric',
                                    ContextImpl.user_metrics_label_names)

  # Called on a per message basis to set the context for the current message
  def set_current_message_context(self, msgid, topic):
    self.current_message_id = msgid
    self.current_input_topic_name = topic
    self.current_start_time = time.time()

  def get_message_id(self):
    return self.current_message_id

  def get_current_message_topic_name(self):
    return self.current_input_topic_name

  def get_function_name(self):
    return self.instance_config.function_details.name

  def get_function_tenant(self):
    return self.instance_config.function_details.tenant

  def get_function_namespace(self):
    return self.instance_config.function_details.namespace

  def get_function_id(self):
    return self.instance_config.function_id

  def get_instance_id(self):
    return self.instance_config.instance_id

  def get_function_version(self):
    return self.instance_config.function_version

  def get_logger(self):
    return self.log

  def get_user_config_value(self, key):
    if key in self.user_config:
      return self.user_config[key]
    else:
      return None
  
  def get_user_config_map(self):
    return self.user_config

  def get_secret(self, secret_key):
    if not secret_key in self.secrets_map:
      return None
    return self.secrets_provider.provide_secret(secret_key, self.secrets_map[secret_key])

  def record_metric(self, metric_name, metric_value):
    if metric_name not in self.user_metrics_labels:
      self.user_metrics_labels[metric_name] = self.metrics_labels + [metric_name]
    self.user_metrics_gauge.labels(*self.user_metrics_labels[metric_name]).set(metric_value)
    if not metric_name in self.accumulated_metrics:
      self.accumulated_metrics[metric_name] = AccumulatedMetricDatum()
    self.accumulated_metrics[metric_name].update(metric_value)

  def get_output_topic(self):
    return self.instance_config.function_details.output

  def get_output_serde_class_name(self):
    return self.instance_config.function_details.outputSerdeClassName

  def publish(self, topic_name, message, serde_class_name="serde.IdentitySerDe", properties=None, compression_type=None):
    # Just make sure that user supplied values are properly typed
    topic_name = str(topic_name)
    serde_class_name = str(serde_class_name)
    pulsar_compression_type = pulsar._pulsar.CompressionType.NONE
    if compression_type is not None:
      pulsar_compression_type = compression_type
    if topic_name not in self.publish_producers:
      self.publish_producers[topic_name] = self.pulsar_client.create_producer(
        topic_name,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000,
        compression_type=pulsar_compression_type
      )

    if serde_class_name not in self.publish_serializers:
      serde_klass = util.import_class(self.user_code_dir, serde_class_name)
      self.publish_serializers[serde_class_name] = serde_klass()

    output_bytes = bytes(self.publish_serializers[serde_class_name].serialize(message))
    self.publish_producers[topic_name].send_async(output_bytes, None, properties=properties)

  def ack(self, msgid, topic):
    if topic not in self.consumers:
      raise ValueError('Invalid topicname %s' % topic)
    self.consumers[topic].acknowledge(msgid)

  def get_and_reset_metrics(self):
    metrics = self.get_metrics()
    # TODO(sanjeev):- Make this thread safe
    self.reset_metrics()
    return metrics

  def reset_metrics(self):
    # TODO: Make it thread safe
    for labels in self.user_metrics_labels.values():
      self.user_metrics_gauge.labels(*labels).set(0.0)
    self.accumulated_metrics.clear()

  def get_metrics(self):
    metrics = InstanceCommunication_pb2.MetricsData()
    for metric_name, accumulated_metric in self.accumulated_metrics.items():
      metrics.metrics[metric_name].count = accumulated_metric.count
      metrics.metrics[metric_name].sum = accumulated_metric.sum
      metrics.metrics[metric_name].max = accumulated_metric.max
      metrics.metrics[metric_name].min = accumulated_metric.min
    return metrics
