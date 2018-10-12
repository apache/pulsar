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
import base64
import os
import signal
import time
try:
  import Queue as queue
except:
  import queue
import threading
from functools import partial
from collections import namedtuple
from threading import Timer
import traceback
import sys
import re

import pulsar
import contextimpl
import Function_pb2
import log
import util
import InstanceCommunication_pb2

Log = log.Log
# Equivalent of the InstanceConfig in Java
InstanceConfig = namedtuple('InstanceConfig', 'instance_id function_id function_version function_details max_buffered_tuples')
# This is the message that the consumers put on the queue for the function thread to process
InternalMessage = namedtuple('InternalMessage', 'message topic serde consumer')
InternalQuitMessage = namedtuple('InternalQuitMessage', 'quit')
DEFAULT_SERIALIZER = "serde.IdentitySerDe"

PY3 = sys.version_info[0] >= 3

def base64ify(bytes_or_str):
    if PY3 and isinstance(bytes_or_str, str):
        input_bytes = bytes_or_str.encode('utf8')
    else:
        input_bytes = bytes_or_str

    output_bytes = base64.urlsafe_b64encode(input_bytes)
    if PY3:
        return output_bytes.decode('ascii')
    else:
        return output_bytes

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

  def update(self, object):
    self.nprocessed = object.nprocessed
    self.nsuccessfullyprocessed = object.nsuccessfullyprocessed
    self.nuserexceptions = object.nuserexceptions
    self.nsystemexceptions = object.nsystemexceptions
    self.nserialization_exceptions = object.nserialization_exceptions
    self.latency = object.latency
    self.lastinvocationtime = object.lastinvocationtime
    self.latestuserexceptions = []
    self.latestsystemexceptions = []
    self.ndeserialization_exceptions.clear()
    self.latestuserexceptions.append(object.latestuserexceptions)
    self.latestsystemexceptions.append(object.latestsystemexceptions)
    self.ndeserialization_exceptions.update(object.ndeserialization_exceptions)
    

class PythonInstance(object):
  def __init__(self, instance_id, function_id, function_version, function_details, max_buffered_tuples, expected_healthcheck_interval, user_code, pulsar_client):
    self.instance_config = InstanceConfig(instance_id, function_id, function_version, function_details, max_buffered_tuples)
    self.user_code = user_code
    self.queue = queue.Queue(max_buffered_tuples)
    self.log_topic_handler = None
    if function_details.logTopic is not None and function_details.logTopic != "":
      self.log_topic_handler = log.LogTopicHandler(str(function_details.logTopic), pulsar_client)
    self.pulsar_client = pulsar_client
    self.input_serdes = {}
    self.consumers = {}
    self.output_serde = None
    self.function_class = None
    self.function_purefunction = None
    self.producer = None
    self.exeuction_thread = None
    self.atmost_once = self.instance_config.function_details.processingGuarantees == Function_pb2.ProcessingGuarantees.Value('ATMOST_ONCE')
    self.atleast_once = self.instance_config.function_details.processingGuarantees == Function_pb2.ProcessingGuarantees.Value('ATLEAST_ONCE')
    self.auto_ack = self.instance_config.function_details.autoAck
    self.contextimpl = None
    self.total_stats = Stats()
    self.current_stats = Stats()
    self.stats = Stats()
    self.last_health_check_ts = time.time()
    self.timeout_ms = function_details.source.timeoutMs if function_details.source.timeoutMs > 0 else None
    self.expected_healthcheck_interval = expected_healthcheck_interval

  def health_check(self):
    self.last_health_check_ts = time.time()
    health_check_result = InstanceCommunication_pb2.HealthCheckResult()
    health_check_result.success = True
    return health_check_result

  def process_spawner_health_check_timer(self):
    if time.time() - self.last_health_check_ts > self.expected_healthcheck_interval * 3:
      Log.critical("Haven't received health check from spawner in a while. Stopping instance...")
      os.kill(os.getpid(), signal.SIGKILL)
      sys.exit(1)

    Timer(self.expected_healthcheck_interval, self.process_spawner_health_check_timer).start()

  def run(self):
    # Setup consumers and input deserializers
    mode = pulsar._pulsar.ConsumerType.Shared
    if self.instance_config.function_details.source.subscriptionType == Function_pb2.SubscriptionType.Value("FAILOVER"):
      mode = pulsar._pulsar.ConsumerType.Failover

    subscription_name = str(self.instance_config.function_details.tenant) + "/" + \
                        str(self.instance_config.function_details.namespace) + "/" + \
                        str(self.instance_config.function_details.name)
    for topic, serde in self.instance_config.function_details.source.topicsToSerDeClassName.items():
      if not serde:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      else:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), serde)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      self.consumers[topic] = self.pulsar_client.subscribe(
        str(topic), subscription_name,
        consumer_type=mode,
        message_listener=partial(self.message_listener, self.input_serdes[topic]),
        unacked_messages_timeout_ms=int(self.timeout_ms) if self.timeout_ms else None
      )

    for topic, consumer_conf in self.instance_config.function_details.source.inputSpecs.items():
      if not consumer_conf.serdeClassName:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      else:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), consumer_conf.serdeClassName)
      self.input_serdes[topic] = serde_kclass()
      Log.info("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
      if consumer_conf.isRegexPattern:
        self.consumers[topic] = self.pulsar_client.subscribe(
          re.compile(str(topic)), subscription_name,
          consumer_type=mode,
          message_listener=partial(self.message_listener, self.input_serdes[topic]),
          unacked_messages_timeout_ms=int(self.timeout_ms) if self.timeout_ms else None
        )
      else:
        self.consumers[topic] = self.pulsar_client.subscribe(
          str(topic), subscription_name,
          consumer_type=mode,
          message_listener=partial(self.message_listener, self.input_serdes[topic]),
          unacked_messages_timeout_ms=int(self.timeout_ms) if self.timeout_ms else None
        )

    function_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_details.className)
    if function_kclass is None:
      Log.critical("Could not import User Function Module %s" % self.instance_config.function_details.className)
      raise NameError("Could not import User Function Module %s" % self.instance_config.function_details.className)
    try:
      self.function_class = function_kclass()
    except:
      self.function_purefunction = function_kclass

    self.contextimpl = contextimpl.ContextImpl(self.instance_config, Log, self.pulsar_client, self.user_code, self.consumers)
    # Now launch a thread that does execution
    self.exeuction_thread = threading.Thread(target=self.actual_execution)
    self.exeuction_thread.start()

    # start proccess spawner health check timer
    self.last_health_check_ts = time.time()
    if self.expected_healthcheck_interval > 0:
      Timer(self.expected_healthcheck_interval, self.process_spawner_health_check_timer).start()

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
      if self.log_topic_handler is not None:
        self.saved_log_handler = log.remove_all_handlers()
        log.add_handler(self.log_topic_handler)
      start_time = time.time()
      self.current_stats.increment_processed(int(start_time) * 1000)
      self.total_stats.increment_processed(int(start_time) * 1000)
      successfully_executed = False
      try:
        if self.function_class is not None:
          output_object = self.function_class.process(input_object, self.contextimpl)
        else:
          output_object = self.function_purefunction.process(input_object)
        successfully_executed = True
      except Exception as e:
        Log.exception("Exception while executing user method")
        self.total_stats.record_user_exception(e)
        self.current_stats.record_user_exception(e)
      end_time = time.time()
      latency = (end_time - start_time) * 1000
      self.total_stats.increment_successfully_processed(latency)
      self.current_stats.increment_successfully_processed(latency)
      if self.log_topic_handler is not None:
        log.remove_all_handlers()
        log.add_handler(self.saved_log_handler)
      if successfully_executed:
        self.process_result(output_object, msg)

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
        props = {"__pfn_input_topic__" : str(msg.topic), "__pfn_input_msg_id__" : base64ify(msg.message.message_id().serialize())}
        try:
          self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message), properties=props)
        except Exception as e:
          self.current_stats.record_system_exception(e)
          self.total_stats.record_system_exception(e)
    elif self.auto_ack and self.atleast_once:
      msg.consumer.acknowledge(msg.message)

  def setup_output_serde(self):
    if self.instance_config.function_details.sink.serDeClassName != None and \
            len(self.instance_config.function_details.sink.serDeClassName) > 0:
      serde_kclass = util.import_class(os.path.dirname(self.user_code), self.instance_config.function_details.sink.serDeClassName)
      self.output_serde = serde_kclass()
    else:
      global DEFAULT_SERIALIZER
      serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      self.output_serde = serde_kclass()

  def setup_producer(self):
    if self.instance_config.function_details.sink.topic != None and \
            len(self.instance_config.function_details.sink.topic) > 0:
      Log.info("Setting up producer for topic %s" % self.instance_config.function_details.sink.topic)
      self.producer = self.pulsar_client.create_producer(
        str(self.instance_config.function_details.sink.topic),
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000)

  def message_listener(self, serde, consumer, message):
    item = InternalMessage(message, consumer.topic(), serde, consumer)
    self.queue.put(item, True)
    if self.atmost_once and self.auto_ack:
      consumer.acknowledge(message)

  def get_and_reset_metrics(self):
    # First get any user metrics
    metrics = self.get_metrics()
    self.reset_metrics()
    return metrics

  def reset_metrics(self):
    self.stats.update(self.current_stats)
    self.current_stats.reset()
    self.contextimpl.reset_metrics()

  def get_metrics(self):
    # First get any user metrics
    metrics = self.contextimpl.get_metrics()
    # Now add system metrics as well
    self.add_system_metrics("__total_processed__", self.stats.nprocessed, metrics)
    self.add_system_metrics("__total_successfully_processed__", self.stats.nsuccessfullyprocessed, metrics)
    self.add_system_metrics("__total_system_exceptions__", self.stats.nsystemexceptions, metrics)
    self.add_system_metrics("__total_user_exceptions__", self.stats.nuserexceptions, metrics)
    for (topic, metric) in self.stats.ndeserialization_exceptions.items():
      self.add_system_metrics("__total_deserialization_exceptions__" + topic, metric, metrics)
    self.add_system_metrics("__total_serialization_exceptions__", self.stats.nserialization_exceptions, metrics)
    self.add_system_metrics("__avg_latency_ms__", self.stats.compute_latency(), metrics)
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
    status.metrics.CopyFrom(self.get_metrics())
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.exeuction_thread.join()
