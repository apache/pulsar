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
    self.nprocessed = 0
    self.nsuccessfullyprocessed = 0
    self.nuserexceptions = 0
    self.ntimeoutexceptions = 0
    self.nsystemexceptions = 0

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
    self.stats = Stats()

  def run(self):
    # Setup consumers and input deserializers
    for topic, serde in self.instance_config.function_config.inputs.items():
      serde_kclass = util.import_class(os.path.dirname(self.user_code), serde, try_internal=True)
      self.input_serdes[topic] = serde_kclass()
      mode = pulsar._pulsar.ConsumerType.Exclusive
      if self.atmost_once:
        mode = pulsar._pulsar.ConsumerType.Shared
      subscription_name = str(self.instance_config.function_config.tenant) + "/" + \
                          str(self.instance_config.function_config.namespace) + "/" + \
                          str(self.instance_config.function_config.name)
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
      try:
        input_object = msg.serde.deserialize(msg.message.data())
        self.contextimpl.set_current_message_context(msg.message.message_id(), msg.topic)
        try:
          output_object = self.function_class.process(input_object, self.contextimpl)
          self.process_result(output_object, msg)
        except Exception as e:
          Log.exception("Exception while executing user method")
          user_exception = True
      except Exception as e:
        Log.exception("System exception while executing method")
        system_exception = True
      self.stats.nprocessed += 1
      if user_exception:
        self.stats.nuserexceptions += 1
      elif system_exception:
        self.stats.nsystemexceptions +=1
      else:
        self.stats.nsuccessfullyprocessed +=1

  def done_producing(self, consumer, orig_message, result, sent_message):
    if result == pulsar.Result.Ok and self.auto_ack and self.atleast_once:
      consumer.acknowledge(orig_message)

  def process_result(self, output, msg):
    if output is not None and self.producer is not None:
      output_bytes = self.output_serde.serialize(output)
      self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message))
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
    self.add_system_metrics("__total_processed__", self.stats.nprocessed, metrics)
    self.add_system_metrics("__total_successfully_processed__", self.stats.nsuccessfullyprocessed, metrics)
    self.add_system_metrics("__total_system_exceptions__", self.stats.nsystemexceptions, metrics)
    self.add_system_metrics("__total_timeout_exceptions__", self.stats.ntimeoutexceptions, metrics)
    self.add_system_metrics("__total_user_exceptions__", self.stats.nuserexceptions, metrics)
    return metrics

  def add_system_metrics(self, metric_name, value, metrics):
    metrics.metrics[metric_name].count = value
    metrics.metrics[metric_name].sum = value
    metrics.metrics[metric_name].min = 0
    metrics.metrics[metric_name].max = value


  def get_function_status(self):
    status = InstanceCommunication_pb2.FunctionStatus()
    status.running = True
    status.numProcessed = self.stats.nprocessed
    status.numSuccessfullyProcessed = self.stats.nsuccessfullyprocessed
    status.numTimeouts = self.stats.ntimeoutexceptions
    status.numUserExceptions = self.stats.nuserexceptions
    status.numSystemExceptions = self.stats.nsystemexceptions
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.exeuction_thread.join()
