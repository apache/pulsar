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
"""python_instance.py: Python Instance for running python functions
"""
import os
import time
import Queue
import threading
from functools import partial
from collections import namedtuple

import pulsar
import contextimpl
import Function_pb2
import log
import util
import InstanceCommunication_pb2

Log = log.Log
# Equivalent of the InstanceConfig in Java
InstanceConfig = namedtuple('InstanceConfig', 'instance_id function_id function_version function_config limits')
# This is the message that the consumers put on the queue for the function thread to process
InternalMessage = namedtuple('InternalMessage', 'message topic serde consumer')
InternalQuitMessage = namedtuple('InternalQuitMessage', 'quit')

# We keep track of the following metrics
class Stats(object):
  def __init__(self):
    self.reset()

  def reset(self):
    self.nprocessed = 0
    self.nsuccessfullyprocessed = 0
    self.nuserexceptions = 0
    self.ntimeoutexceptions = 0
    self.nsystemexceptions = 0
    self.ndeserialization_exceptions = {}
    self.nserialization_exceptions = 0
    self.latency = 0

  def increment_deser_errors(self, topic):
    if topic not in self.ndeserialization_exceptions:
      self.ndeserialization_exceptions[topic] = 0
    self.ndeserialization_exceptions[topic] += 1

  def increment_successfully_processed(self, latency):
    self.nsuccessfullyprocessed += 1
    self.latency += latency

  def compute_latency(self):
    if self.nsuccessfullyprocessed <= 0:
      return 0
    else:
      return self.latency / self.nsuccessfullyprocessed

class PythonInstance(object):
  def __init__(self, instance_id, function_id, function_version, function_config, limits, user_code, pulsar_client):
    self.instance_config = InstanceConfig(instance_id, function_id, function_version, function_config, limits)
    self.user_code = user_code
    self.queue = Queue.Queue(limits.max_buffered_tuples)
    self.pulsar_client = pulsar_client
    self.input_serdes = {}
    self.consumers = {}
    self.output_serde = None
    self.function_class = None
    self.producer = None
    self.exeuction_thread = None
    self.atmost_once = self.instance_config.function_config.processingGuarantees == Function_pb2.FunctionConfig.ProcessingGuarantees.Value('ATMOST_ONCE')
    self.atleast_once = self.instance_config.function_config.processingGuarantees == Function_pb2.FunctionConfig.ProcessingGuarantees.Value('ATLEAST_ONCE')
    self.auto_ack = self.instance_config.function_config.autoAck
    self.contextimpl = None
    self.total_stats = Stats()
    self.current_stats = Stats()

  def run(self):
    # Setup consumers and input deserializers
    mode = pulsar._pulsar.ConsumerType.Exclusive
    if self.atmost_once:
      mode = pulsar._pulsar.ConsumerType.Shared
    subscription_name = str(self.instance_config.function_config.tenant) + "/" + \
                        str(self.instance_config.function_config.namespace) + "/" + \
                        str(self.instance_config.function_config.name)
    for topic, serde in self.instance_config.function_config.custom_serde_inputs.items():
      serde_kclass = util.import_class(os.path.dirname(self.user_code), serde, try_internal=True)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      self.consumers[topic] = self.pulsar_client.subscribe(
        str(topic), subscription_name,
        consumer_type=mode,
        message_listener=partial(self.message_listener, topic, self.input_serdes[topic])
      )

    for topic in self.instance_config.function_config.inputs:
      serde_kclass = util.import_class(os.path.dirname(self.user_code), "pulsarfunction.IdentitySerDe", try_internal=True)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      self.consumers[topic] = self.pulsar_client.subscribe(
        str(topic), subscription_name,
        consumer_type=mode,
        message_listener=partial(self.message_listener, topic, self.input_serdes[topic])
      )

    # See if we need to setup output producers/output serializers
    if self.instance_config.function_config.outputSerdeClassName != None and \
      len(self.instance_config.function_config.outputSerdeClassName) > 0:
      serde_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_config.outputSerdeClassName, try_internal=True)
      self.output_serde = serde_kclass()
    function_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_config.className, try_internal=True)
    self.function_class = function_kclass()
    if self.instance_config.function_config.sinkTopic != None and \
      len(self.instance_config.function_config.sinkTopic) > 0:
      Log.info("Setting up producer for topic %s" % self.instance_config.function_config.sinkTopic)
      self.producer = self.pulsar_client.create_producer(
        str(self.instance_config.function_config.sinkTopic),
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000)

    self.contextimpl = contextimpl.ContextImpl(self.instance_config, Log, self.pulsar_client, self.user_code, self.consumers)
    # Now launch a thread that does execution
    self.exeuction_thread = threading.Thread(target=self.actual_execution)
    self.exeuction_thread.start()

  def actual_execution(self):
    Log.info("Started Thread for executing the function")
    while True:
      msg = self.queue.get(True)
      if isinstance(msg, InternalQuitMessage):
        break
      user_exception = False
      system_exception = False
      Log.debug("Got a message from topic %s" % msg.topic)
      self.current_stats.nprocessed += 1
      self.total_stats.nprocessed += 1
      input_object = None
      try:
        input_object = msg.serde.deserialize(msg.message.data())
      except:
        self.current_stats.increment_deser_errors(msg.topic)
        self.total_stats.increment_deser_errors(msg.topic)
        continue
      self.contextimpl.set_current_message_context(msg.message.message_id(), msg.topic)
      output_object = None
      try:
        start_time = time.time()
        output_object = self.function_class.process(input_object, self.contextimpl)
        end_time = time.time()
        latency = (end_time - start_time) * 1000
        self.total_stats.increment_successfully_processed(latency)
        self.current_stats.increment_successfully_processed(latency)
        self.process_result(output_object, msg)
      except Exception as e:
        Log.exception("Exception while executing user method")
        self.total_stats.nuserexceptions += 1
        self.current_stats.nuserexceptions += 1

  def done_producing(self, consumer, orig_message, result, sent_message):
    if result == pulsar.Result.Ok and self.auto_ack and self.atleast_once:
      consumer.acknowledge(orig_message)

  def process_result(self, output, msg):
    if output is not None and self.producer is not None:
      output_bytes = None
      try:
        output_bytes = self.output_serde.serialize(output)
      except:
        self.current_stats.nserialization_exceptions += 1
        self.total_stats.nserialization_exceptions += 1
      if output_bytes is not None:
        try:
          self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message))
        except:
          self.current_stats.nsystemexceptions += 1
          self.total_stats.nsystemexceptions += 1
    elif self.auto_ack and self.atleast_once:
      msg.consumer.acknowledge(msg.message)

  def message_listener(self, topic, serde, consumer, message):
    item = InternalMessage(message, topic, serde, consumer)
    self.queue.put(item, True)
    if self.atmost_once and self.auto_ack:
      consumer.acknowledge(message)

  def get_and_reset_metrics(self):
    # First get any user metrics
    metrics = self.contextimpl.get_and_reset_metrics()
    # Now add system metrics as well
    self.add_system_metrics("__total_processed__", self.current_stats.nprocessed, metrics)
    self.add_system_metrics("__total_successfully_processed__", self.current_stats.nsuccessfullyprocessed, metrics)
    self.add_system_metrics("__total_system_exceptions__", self.current_stats.nsystemexceptions, metrics)
    self.add_system_metrics("__total_timeout_exceptions__", self.current_stats.ntimeoutexceptions, metrics)
    self.add_system_metrics("__total_user_exceptions__", self.current_stats.nuserexceptions, metrics)
    for (topic, metric) in self.current_stats.ndeserialization_exceptions.items():
      self.add_system_metrics("__total_deserialization_exceptions__" + topic, metric, metrics)
    self.add_system_metrics("__total_serialization_exceptions__", self.current_stats.nserialization_exceptions, metrics)
    self.add_system_metrics("__avg_latency_ms__", self.current_stats.compute_latency(), metrics)
    self.current_stats.reset()
    return metrics

  def add_system_metrics(self, metric_name, value, metrics):
    metrics.metrics[metric_name].count = value
    metrics.metrics[metric_name].sum = value
    metrics.metrics[metric_name].min = 0
    metrics.metrics[metric_name].max = value

  def get_function_status(self):
    status = InstanceCommunication_pb2.FunctionStatus()
    status.running = True
    status.numProcessed = self.total_stats.nprocessed
    status.numSuccessfullyProcessed = self.total_stats.nsuccessfullyprocessed
    status.numTimeouts = self.total_stats.ntimeoutexceptions
    status.numUserExceptions = self.total_stats.nuserexceptions
    status.numSystemExceptions = self.total_stats.nsystemexceptions
    for (topic, metric) in self.total_stats.ndeserialization_exceptions.items():
      status.deserializationExceptions[topic] = metric
    status.serializationExceptions = self.total_stats.nserialization_exceptions
    status.averageLatency = self.total_stats.compute_latency()
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.exeuction_thread.join()
