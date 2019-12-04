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

import re
import time
import os
import json
import log

import pulsar
import util

from prometheus_client import Summary
from function_stats import Stats
from functools import partial

Log = log.Log

class ContextImpl(pulsar.Context):

  # add label to indicate user metric
  user_metrics_label_names = Stats.metrics_label_names + ["metric"]

  def __init__(self, instance_config, logger, pulsar_client, user_code, consumers,
               secrets_provider, metrics_labels, state_context, stats):
    self.instance_config = instance_config
    self.log = logger
    self.pulsar_client = pulsar_client
    self.user_code_dir = os.path.dirname(user_code)
    self.consumers = consumers
    self.secrets_provider = secrets_provider
    self.state_context = state_context
    self.publish_producers = {}
    self.publish_serializers = {}
    self.message = None
    self.current_start_time = None
    self.user_config = json.loads(instance_config.function_details.userConfig) \
      if instance_config.function_details.userConfig \
      else []
    self.secrets_map = json.loads(instance_config.function_details.secretsMap) \
      if instance_config.function_details.secretsMap \
      else {}

    self.metrics_labels = metrics_labels
    self.user_metrics_map = dict()
    self.user_metrics_summary = Summary("pulsar_function_user_metric",
                                    'Pulsar Function user defined metric',
                                        ContextImpl.user_metrics_label_names)
    self.stats = stats

  # Called on a per message basis to set the context for the current message
  def set_current_message_context(self, message, topic):
    self.message = message
    self.current_start_time = time.time()

  def get_message_id(self):
    return self.message.message_id()

  def get_message_key(self):
    return self.message.partition_key()

  def get_message_eventtime(self):
    return self.message.event_timestamp()

  def get_message_properties(self):
    return self.message.properties()

  def get_current_message_topic_name(self):
    return self.message.topic_name()

  def get_partition_key(self):
    return self.message.partition_key()

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
    if metric_name not in self.user_metrics_map:
      user_metrics_labels = self.metrics_labels + [metric_name]
      self.user_metrics_map[metric_name] = self.user_metrics_summary.labels(*user_metrics_labels)

    self.user_metrics_map[metric_name].observe(metric_value)

  def get_input_topics(self):
    return list(self.instance_config.function_details.source.inputSpecs.keys())

  def get_output_topic(self):
    return self.instance_config.function_details.output

  def get_output_serde_class_name(self):
    return self.instance_config.function_details.outputSerdeClassName

  def callback_wrapper(self, callback, topic, message_id, result, msg):
    if result != pulsar.Result.Ok:
      error_msg = "Failed to publish to topic [%s] with error [%s] with src message id [%s]" % (topic, result, message_id)
      Log.error(error_msg)
      self.stats.incr_total_sys_exceptions(Exception(error_msg))
    if callback:
      callback(result, msg)

  def publish(self, topic_name, message, serde_class_name="serde.IdentitySerDe", properties=None, compression_type=None, callback=None, message_conf=None):
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
        batching_max_publish_delay_ms=10,
        compression_type=pulsar_compression_type,
        properties=util.get_properties(util.getFullyQualifiedFunctionName(
          self.instance_config.function_details.tenant,
          self.instance_config.function_details.namespace,
          self.instance_config.function_details.name),
          self.instance_config.instance_id)
      )

    if serde_class_name not in self.publish_serializers:
      serde_klass = util.import_class(self.user_code_dir, serde_class_name)
      self.publish_serializers[serde_class_name] = serde_klass()

    output_bytes = bytes(self.publish_serializers[serde_class_name].serialize(message))

    if properties:
      # The deprecated properties args was passed. Need to merge into message_conf
      if not message_conf:
        message_conf = {}
      message_conf['properties'] = properties

    if message_conf:
      self.publish_producers[topic_name].send_async(
        output_bytes, partial(self.callback_wrapper, callback, topic_name, self.get_message_id()), **message_conf)
    else:
      self.publish_producers[topic_name].send_async(
        output_bytes, partial(self.callback_wrapper, callback, topic_name, self.get_message_id()))

  def ack(self, msgid, topic):
    topic_consumer = None
    if topic in self.consumers:
      topic_consumer = self.consumers[topic]
    else:
      # if this topic is a partitioned topic
      m = re.search('(.+)-partition-(\d+)', topic)
      if not m:
        raise ValueError('Invalid topicname %s' % topic)
      elif m.group(1) in self.consumers:
        topic_consumer = self.consumers[m.group(1)]
      else:
        raise ValueError('Invalid topicname %s' % topic)
    topic_consumer.acknowledge(msgid)

  def get_and_reset_metrics(self):
    metrics = self.get_metrics()
    # TODO(sanjeev):- Make this thread safe
    self.reset_metrics()
    return metrics

  def reset_metrics(self):
    # TODO: Make it thread safe
    for user_metric in self.user_metrics_map.values():
      user_metric._sum.set(0.0)
      user_metric._count.set(0.0)

  def get_metrics(self):
    metrics_map = {}
    for metric_name, user_metric in self.user_metrics_map.items():
      metrics_map["%s%s_sum" % (Stats.USER_METRIC_PREFIX, metric_name)] = user_metric._sum.get()
      metrics_map["%s%s_count" % (Stats.USER_METRIC_PREFIX, metric_name)] = user_metric._count.get()

    return metrics_map

  def incr_counter(self, key, amount):
    return self.state_context.incr(key, amount)

  def get_counter(self, key):
    return self.state_context.get_amount(key)

  def del_counter(self, key):
    return self.state_context.delete(key)

  def put_state(self, key, value):
    return self.state_context.put(key, value)

  def get_state(self, key):
    return self.state_context.get_value(key)
