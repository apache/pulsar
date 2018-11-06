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
import sys
import re
import pulsar
import contextimpl
import Function_pb2
import log
import util
import InstanceCommunication_pb2

from functools import partial
from collections import namedtuple
from threading import Timer
from function_stats import Stats

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

class PythonInstance(object):
  def __init__(self, instance_id, function_id, function_version, function_details, max_buffered_tuples,
               expected_healthcheck_interval, user_code, pulsar_client, secrets_provider, cluster_name):
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
    self.execution_thread = None
    self.atmost_once = self.instance_config.function_details.processingGuarantees == Function_pb2.ProcessingGuarantees.Value('ATMOST_ONCE')
    self.atleast_once = self.instance_config.function_details.processingGuarantees == Function_pb2.ProcessingGuarantees.Value('ATLEAST_ONCE')
    self.auto_ack = self.instance_config.function_details.autoAck
    self.contextimpl = None
    self.stats = Stats()
    self.last_health_check_ts = time.time()
    self.timeout_ms = function_details.source.timeoutMs if function_details.source.timeoutMs > 0 else None
    self.expected_healthcheck_interval = expected_healthcheck_interval
    self.secrets_provider = secrets_provider
    self.metrics_labels = [function_details.tenant,
                           "%s/%s" % (function_details.tenant, function_details.namespace),
                           function_details.name, instance_id, cluster_name]

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
      Log.debug("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
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
      Log.debug("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))
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

    self.contextimpl = contextimpl.ContextImpl(self.instance_config, Log, self.pulsar_client,
                                               self.user_code, self.consumers,
                                               self.secrets_provider, self.metrics_labels)
    # Now launch a thread that does execution
    self.execution_thread = threading.Thread(target=self.actual_execution)
    self.execution_thread.start()

    # start proccess spawner health check timer
    self.last_health_check_ts = time.time()
    if self.expected_healthcheck_interval > 0:
      Timer(self.expected_healthcheck_interval, self.process_spawner_health_check_timer).start()

  def actual_execution(self):
    Log.debug("Started Thread for executing the function")

    while True:
      try:
        msg = self.queue.get(True)
        if isinstance(msg, InternalQuitMessage):
          break
        Log.debug("Got a message from topic %s" % msg.topic)
        # deserialize message
        input_object = msg.serde.deserialize(msg.message.data())
        # set current message in context
        self.contextimpl.set_current_message_context(msg.message.message_id(), msg.topic)
        output_object = None
        self.saved_log_handler = None
        if self.log_topic_handler is not None:
          self.saved_log_handler = log.remove_all_handlers()
          log.add_handler(self.log_topic_handler)
        successfully_executed = False
        try:
          # get user function start time for statistic calculation
          start_time = time.time()
          Stats.stat_last_invocation.labels(*self.metrics_labels).set(start_time * 1000.0)
          if self.function_class is not None:
            output_object = self.function_class.process(input_object, self.contextimpl)
          else:
            output_object = self.function_purefunction.process(input_object)
          successfully_executed = True
          Stats.stat_process_latency_ms.labels(*self.metrics_labels).observe((time.time() - start_time) * 1000.0)
          Stats.stat_total_processed.labels(*self.metrics_labels).inc()
        except Exception as e:
          Log.exception("Exception while executing user method")
          Stats.stat_total_user_exceptions.labels(*self.metrics_labels).inc()
          self.stats.add_user_exception()

        if self.log_topic_handler is not None:
          log.remove_all_handlers()
          log.add_handler(self.saved_log_handler)
        if successfully_executed:
          self.process_result(output_object, msg)
          Stats.stat_total_processed_successfully.labels(*self.metrics_labels).inc()

      except Exception as e:
        Log.error("Uncaught exception in Python instance: %s" % e);
        Stats.stat_total_sys_exceptions.labels(*self.metrics_labels).inc()
        self.stats.add_sys_exception()

  def done_producing(self, consumer, orig_message, result, sent_message):
    if result == pulsar.Result.Ok and self.auto_ack and self.atleast_once:
      consumer.acknowledge(orig_message)

  def process_result(self, output, msg):
    if output is not None:
      if self.output_serde is None:
        self.setup_output_serde()
      if self.producer is None:
        self.setup_producer()

      # serialize function output
      output_bytes = self.output_serde.serialize(output)

      if output_bytes is not None:
        props = {"__pfn_input_topic__" : str(msg.topic), "__pfn_input_msg_id__" : base64ify(msg.message.message_id().serialize())}
        self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message), properties=props)
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
      Log.debug("Setting up producer for topic %s" % self.instance_config.function_details.sink.topic)
      self.producer = self.pulsar_client.create_producer(
        str(self.instance_config.function_details.sink.topic),
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=1,
        max_pending_messages=100000)

  def message_listener(self, serde, consumer, message):
    # increment number of received records from source
    Stats.stat_total_received.labels(*self.metrics_labels).inc()
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
    self.stats.reset(self.metrics_labels)
    self.contextimpl.reset_metrics()

  def get_metrics(self):
    # First get any user metrics
    metrics = self.contextimpl.get_metrics()
    # Now add system metrics as well
    self.add_system_metrics(Stats.TOTAL_PROCESSED, Stats.stat_total_processed.labels(*self.metrics_labels)._value.get(), metrics)
    self.add_system_metrics(Stats.TOTAL_SUCCESSFULLY_PROCESSED, Stats.stat_total_processed_successfully.labels(*self.metrics_labels)._value.get(), metrics)
    self.add_system_metrics(Stats.TOTAL_SYSTEM_EXCEPTIONS, Stats.stat_total_sys_exceptions.labels(*self.metrics_labels)._value.get(), metrics)
    self.add_system_metrics(Stats.TOTAL_USER_EXCEPTIONS, Stats.stat_total_user_exceptions.labels(*self.metrics_labels)._value.get(), metrics)
    self.add_system_metrics(Stats.PROCESS_LATENCY_MS,
                            0.0 if Stats.stat_process_latency_ms.labels(*self.metrics_labels)._count.get() <= 0.0
                            else Stats.stat_process_latency_ms.labels(*self.metrics_labels)._sum.get() / Stats.stat_process_latency_ms.labels(*self.metrics_labels)._count.get(),
                            metrics)
    self.add_system_metrics(Stats.TOTAL_RECEIVED, Stats.stat_total_received.labels(*self.metrics_labels)._value.get(), metrics)
    self.add_system_metrics(Stats.LAST_INVOCATION, Stats.stat_last_invocation.labels(*self.metrics_labels)._value.get(), metrics)
    return metrics

  def add_system_metrics(self, metric_name, value, metrics):
    metrics.metrics[metric_name].count = value
    metrics.metrics[metric_name].sum = value
    metrics.metrics[metric_name].min = 0
    metrics.metrics[metric_name].max = value

  def get_function_status(self):
    status = InstanceCommunication_pb2.FunctionStatus()
    status.running = True

    total_processed = Stats.stat_total_processed.labels(*self.metrics_labels)._value.get()
    stat_total_processed_successfully = Stats.stat_total_processed_successfully.labels(*self.metrics_labels)._value.get()
    stat_total_user_exceptions = Stats.stat_total_user_exceptions.labels(*self.metrics_labels)._value.get()
    stat_total_sys_exceptions = Stats.stat_total_sys_exceptions.labels(*self.metrics_labels)._value.get()
    stat_process_latency_ms_count = Stats.stat_process_latency_ms.labels(*self.metrics_labels)._count.get()
    stat_process_latency_ms_sum = Stats.stat_process_latency_ms.labels(*self.metrics_labels)._sum.get()
    stat_last_invocation = Stats.stat_last_invocation.labels(*self.metrics_labels)._value.get()

    status.numProcessed =  int(total_processed) if sys.version_info.major >= 3 else long(total_processed)
    status.numSuccessfullyProcessed = int(stat_total_processed_successfully) if sys.version_info.major >= 3 else long(stat_total_processed_successfully)
    status.numUserExceptions = int(stat_total_user_exceptions) if sys.version_info.major >= 3 else long(stat_total_user_exceptions)
    status.instanceId = self.instance_config.instance_id
    for ex, tm in self.stats.latest_user_exception:
      to_add = status.latestUserExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    status.numSystemExceptions = int(stat_total_sys_exceptions) if sys.version_info.major >= 3 else long(stat_total_sys_exceptions)
    for ex, tm in self.stats.latest_sys_exception:
      to_add = status.latestSystemExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    status.averageLatency = 0.0 \
      if stat_process_latency_ms_count <= 0.0 \
      else stat_process_latency_ms_sum / stat_process_latency_ms_count
    status.lastInvocationTime = int(stat_last_invocation) if sys.version_info.major >= 3 else long(stat_last_invocation)
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.execution_thread.join()
