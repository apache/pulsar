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

"""python_instance.py: Python Instance for running python functions
"""
import os
import time
import Queue
import threading
from functools import partial
from collections import namedtuple
import traceback

import pulsar
import contextimpl
import Function_pb2
import log
import util
import InstanceCommunication_pb2

Log = log.Log
# Equivalent of the InstanceConfig in Java
InstanceConfig = namedtuple('InstanceConfig', 'instance_id function_id function_version function_config max_buffered_tuples')
# This is the message that the consumers put on the queue for the function thread to process
InternalMessage = namedtuple('InternalMessage', 'message topic serde consumer')
InternalQuitMessage = namedtuple('InternalQuitMessage', 'quit')
DEFAULT_SERIALIZER = "serde.IdentitySerDe"

# We keep track of the following metrics
class Stats(object):
  def __init__(self):
    self.reset()

  def reset(self):
    self.nprocessed = 0
    self.nsuccessfullyprocessed = 0
    self.nuserexceptions = 0
    self.latestuserexceptions = []
    self.nsystemexceptions = 0
    self.latestsystemexceptions = []
    self.ndeserialization_exceptions = {}
    self.nserialization_exceptions = 0
    self.latency = 0
    self.lastinvocationtime = 0

  def increment_deser_errors(self, topic):
    if topic not in self.ndeserialization_exceptions:
      self.ndeserialization_exceptions[topic] = 0
    self.ndeserialization_exceptions[topic] += 1

  def increment_successfully_processed(self, latency):
    self.nsuccessfullyprocessed += 1
    self.latency += latency

  def increment_processed(self, processed_at):
    self.nprocessed += 1
    self.lastinvocationtime = processed_at

  def record_user_exception(self, ex):
    self.latestuserexceptions.append((traceback.format_exc(), int(time.time() * 1000)))
    if len(self.latestuserexceptions) > 10:
      self.latestuserexceptions.pop(0)
    self.nuserexceptions = self.nuserexceptions + 1

  def record_system_exception(self, ex):
    self.latestsystemexceptions.append((traceback.format_exc(), int(time.time() * 1000)))
    if len(self.latestsystemexceptions) > 10:
      self.latestsystemexceptions.pop(0)
    self.nsystemexceptions = self.nsystemexceptions + 1

  def compute_latency(self):
    if self.nsuccessfullyprocessed <= 0:
      return 0
    else:
      return self.latency / self.nsuccessfullyprocessed

class PythonInstance(object):
  def __init__(self, instance_id, function_id, function_version, function_config, max_buffered_tuples, user_code, log_topic, pulsar_client):
    self.instance_config = InstanceConfig(instance_id, function_id, function_version, function_config, max_buffered_tuples)
    self.user_code = user_code
    self.queue = Queue.Queue(max_buffered_tuples)
    self.log_topic_handler = None
    if log_topic is not None:
      self.log_topic_handler = log.LogTopicHandler(str(log_topic), pulsar_client)
    self.pulsar_client = pulsar_client
    self.input_serdes = {}
    self.consumers = {}
    self.output_serde = None
    self.function_class = None
    self.function_purefunction = None
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
    for topic, serde in self.instance_config.function_config.customSerdeInputs.items():
      serde_kclass = util.import_class(os.path.dirname(self.user_code), serde)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      self.consumers[topic] = self.pulsar_client.subscribe(
        str(topic), subscription_name,
        consumer_type=mode,
        message_listener=partial(self.message_listener, topic, self.input_serdes[topic])
      )

    for topic in self.instance_config.function_config.inputs:
      global DEFAULT_SERIALIZER
      serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      self.consumers[topic] = self.pulsar_client.subscribe(
        str(topic), subscription_name,
        consumer_type=mode,
        message_listener=partial(self.message_listener, topic, self.input_serdes[topic])
      )

    function_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_config.className)
    if function_kclass is None:
      Log.critical("Could not import User Function Module %s" % self.instance_config.function_config.className)
      raise NameError("Could not import User Function Module %s" % self.instance_config.function_config.className)
    try:
      self.function_class = function_kclass()
    except:
      self.function_purefunction = function_kclass

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
      input_object = None
      try:
        input_object = msg.serde.deserialize(msg.message.data())
      except:
        self.current_stats.increment_deser_errors(msg.topic)
        self.total_stats.increment_deser_errors(msg.topic)
        continue
      self.contextimpl.set_current_message_context(msg.message.message_id(), msg.topic)
      output_object = None
      self.saved_log_handler = None
      try:
        if self.log_topic_handler is not None:
          self.saved_log_handler = log.remove_all_handlers()
          log.add_handler(self.log_topic_handler)
        start_time = time.time()
        self.current_stats.increment_processed(int(start_time) * 1000)
        self.total_stats.increment_processed(int(start_time) * 1000)
        if self.function_class is not None:
          output_object = self.function_class.process(input_object, self.contextimpl)
        else:
          output_object = self.function_purefunction.process(input_object)
        end_time = time.time()
        latency = (end_time - start_time) * 1000
        self.total_stats.increment_successfully_processed(latency)
        self.current_stats.increment_successfully_processed(latency)
        self.process_result(output_object, msg)
      except Exception as e:
        if self.log_topic_handler is not None:
          log.remove_all_handlers()
          log.add_handler(self.saved_log_handler)
        Log.exception("Exception while executing user method")
        self.total_stats.record_user_exception(e)
        self.current_stats.record_user_exception(e)
      finally:
        if self.log_topic_handler is not None:
          log.remove_all_handlers()
          log.add_handler(self.saved_log_handler)

  def done_producing(self, consumer, orig_message, result, sent_message):
    if result == pulsar.Result.Ok and self.auto_ack and self.atleast_once:
      consumer.acknowledge(orig_message)

  def process_result(self, output, msg):
    if output is not None:
      output_bytes = None
      if self.output_serde is None:
        self.setup_output_serde()
      if self.producer is None:
        self.setup_producer()
      try:
        output_bytes = self.output_serde.serialize(output)
      except:
        self.current_stats.nserialization_exceptions += 1
        self.total_stats.nserialization_exceptions += 1
      if output_bytes is not None:
        try:
          self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message))
        except Exception as e:
          self.current_stats.record_system_exception(e)
          self.total_stats.record_system_exception(e)
    elif self.auto_ack and self.atleast_once:
      msg.consumer.acknowledge(msg.message)

  def setup_output_serde(self):
    if self.instance_config.function_config.outputSerdeClassName != None and \
            len(self.instance_config.function_config.outputSerdeClassName) > 0:
      serde_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_config.outputSerdeClassName)
      self.output_serde = serde_kclass()
    else:
      global DEFAULT_SERIALIZER
      serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      self.output_serde = serde_kclass()

  def setup_producer(self):
    if self.instance_config.function_config.output != None and \
            len(self.instance_config.function_config.output) > 0:
      Log.info("Setting up producer for topic %s" % self.instance_config.function_config.output)
      self.producer = self.pulsar_client.create_producer(
        str(self.instance_config.function_config.output),
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000)

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
    status.numUserExceptions = self.total_stats.nuserexceptions
    status.instanceId = self.instance_config.instance_id
    for ex, tm in self.total_stats.latestuserexceptions:
      to_add = status.latestUserExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    status.numSystemExceptions = self.total_stats.nsystemexceptions
    for ex, tm in self.total_stats.latestsystemexceptions:
      to_add = status.latestSystemExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    for (topic, metric) in self.total_stats.ndeserialization_exceptions.items():
      status.deserializationExceptions[topic] = metric
    status.serializationExceptions = self.total_stats.nserialization_exceptions
    status.averageLatency = self.total_stats.compute_latency()
    status.lastInvocationTime = self.total_stats.lastinvocationtime
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.exeuction_thread.join()
