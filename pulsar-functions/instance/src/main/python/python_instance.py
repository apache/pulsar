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

# state dependencies
import state_context

from functools import partial
from collections import namedtuple
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
  def __init__(self,
               instance_id,
               function_id,
               function_version,
               function_details,
               max_buffered_tuples,
               expected_healthcheck_interval,
               user_code,
               pulsar_client,
               secrets_provider,
               cluster_name,
               state_storage_serviceurl):
    self.instance_config = InstanceConfig(instance_id, function_id, function_version, function_details, max_buffered_tuples)
    self.user_code = user_code
    # set queue size to one since consumers already have internal queues. Just use queue to communicate message from
    # consumers to processing thread
    self.queue = queue.Queue(1)
    self.log_topic_handler = None
    if function_details.logTopic is not None and function_details.logTopic != "":
      self.log_topic_handler = log.LogTopicHandler(str(function_details.logTopic), pulsar_client)
    self.pulsar_client = pulsar_client
    self.state_storage_serviceurl = state_storage_serviceurl
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
    self.last_health_check_ts = time.time()
    self.timeout_ms = function_details.source.timeoutMs if function_details.source.timeoutMs > 0 else None
    self.expected_healthcheck_interval = expected_healthcheck_interval
    self.secrets_provider = secrets_provider
    self.state_context = state_context.NullStateContext()
    self.metrics_labels = [function_details.tenant,
                           "%s/%s" % (function_details.tenant, function_details.namespace),
                           function_details.name,
                           instance_id, cluster_name,
                           "%s/%s/%s" % (function_details.tenant, function_details.namespace, function_details.name)]
    self.stats = Stats(self.metrics_labels)

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

  def run(self):
    # Setup state
    self.state_context = self.setup_state()

    # Setup consumers and input deserializers
    mode = pulsar._pulsar.ConsumerType.Shared
    if self.instance_config.function_details.source.subscriptionType == Function_pb2.SubscriptionType.Value("FAILOVER"):
      mode = pulsar._pulsar.ConsumerType.Failover

    subscription_name = self.instance_config.function_details.source.subscriptionName    

    if not (subscription_name and subscription_name.strip()):
      subscription_name = str(self.instance_config.function_details.tenant) + "/" + \
                          str(self.instance_config.function_details.namespace) + "/" + \
                          str(self.instance_config.function_details.name)

    properties = util.get_properties(util.getFullyQualifiedFunctionName(
                        self.instance_config.function_details.tenant,
                        self.instance_config.function_details.namespace,
                        self.instance_config.function_details.name),
                        self.instance_config.instance_id)

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
        unacked_messages_timeout_ms=int(self.timeout_ms) if self.timeout_ms else None,
        properties=properties
      )

    for topic, consumer_conf in self.instance_config.function_details.source.inputSpecs.items():
      if not consumer_conf.serdeClassName:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), DEFAULT_SERIALIZER)
      else:
        serde_kclass = util.import_class(os.path.dirname(self.user_code), consumer_conf.serdeClassName)
      self.input_serdes[topic] = serde_kclass()
      Log.debug("Setting up consumer for topic %s with subname %s" % (topic, subscription_name))

      consumer_args = {
        "consumer_type": mode,
        "message_listener": partial(self.message_listener, self.input_serdes[topic]),
        "unacked_messages_timeout_ms": int(self.timeout_ms) if self.timeout_ms else None,
        "properties": properties
      }
      if consumer_conf.HasField("receiverQueueSize"):
        consumer_args["receiver_queue_size"] = consumer_conf.receiverQueueSize.value

      if consumer_conf.isRegexPattern:
        self.consumers[topic] = self.pulsar_client.subscribe(
          re.compile(str(topic)), subscription_name,
          **consumer_args
        )
      else:
        self.consumers[topic] = self.pulsar_client.subscribe(
          str(topic), subscription_name,
          **consumer_args
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
                                               self.secrets_provider, self.metrics_labels,
                                               self.state_context, self.stats)
    # Now launch a thread that does execution
    self.execution_thread = threading.Thread(target=self.actual_execution)
    self.execution_thread.start()

    # start proccess spawner health check timer
    self.last_health_check_ts = time.time()
    if self.expected_healthcheck_interval > 0:
      timer = util.FixedTimer(self.expected_healthcheck_interval, self.process_spawner_health_check_timer, name="health-check-timer")
      timer.start()

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
        self.contextimpl.set_current_message_context(msg.message, msg.topic)
        output_object = None
        self.saved_log_handler = None
        if self.log_topic_handler is not None:
          self.saved_log_handler = log.remove_all_handlers()
          log.add_handler(self.log_topic_handler)
        successfully_executed = False
        try:
          # get user function start time for statistic calculation
          self.stats.set_last_invocation(time.time())

          # start timer for process time
          self.stats.process_time_start()
          if self.function_class is not None:
            output_object = self.function_class.process(input_object, self.contextimpl)
          else:
            output_object = self.function_purefunction.process(input_object)
          successfully_executed = True

          # stop timer for process time
          self.stats.process_time_end()
        except Exception as e:
          Log.exception("Exception while executing user method")
          self.stats.incr_total_user_exceptions(e)
          # If function throws exception then send neg ack for input message back to broker
          msg.consumer.negative_acknowledge(msg.message)

        if self.log_topic_handler is not None:
          log.remove_all_handlers()
          log.add_handler(self.saved_log_handler)
        if successfully_executed:
          self.process_result(output_object, msg)
          self.stats.incr_total_processed_successfully()

      except Exception as e:
        Log.error("Uncaught exception in Python instance: %s" % e);
        self.stats.incr_total_sys_exceptions(e)
        if msg:
          msg.consumer.negative_acknowledge(msg.message)

  def done_producing(self, consumer, orig_message, topic, result, sent_message):
    if result == pulsar.Result.Ok:
      if self.auto_ack:
        consumer.acknowledge(orig_message)
    else:
      error_msg = "Failed to publish to topic [%s] with error [%s] with src message id [%s]" % (topic, result, orig_message.message_id())
      Log.error(error_msg)
      self.stats.incr_total_sys_exceptions(Exception(error_msg))
      # If producer fails send output then send neg ack for input message back to broker
      consumer.negative_acknowledge(orig_message)


  def process_result(self, output, msg):
    if output is not None and self.instance_config.function_details.sink.topic != None and \
            len(self.instance_config.function_details.sink.topic) > 0:
      if self.output_serde is None:
        self.setup_output_serde()
      if self.producer is None:
        self.setup_producer()

      # serialize function output
      output_bytes = self.output_serde.serialize(output)

      if output_bytes is not None:
        props = {"__pfn_input_topic__" : str(msg.topic), "__pfn_input_msg_id__" : base64ify(msg.message.message_id().serialize())}
        self.producer.send_async(output_bytes, partial(self.done_producing, msg.consumer, msg.message, self.producer.topic()), properties=props)
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

      batch_type = pulsar.BatchingType.Default
      if self.instance_config.function_details.sink.producerSpec.batchBuilder != None and \
            len(self.instance_config.function_details.sink.producerSpec.batchBuilder) > 0:
        batch_builder = self.instance_config.function_details.sink.producerSpec.batchBuilder
        if batch_builder == "KEY_BASED":
          batch_type = pulsar.BatchingType.KeyBased

      self.producer = self.pulsar_client.create_producer(
        str(self.instance_config.function_details.sink.topic),
        block_if_queue_full=True,
        batching_enabled=True,
        batching_type=batch_type,
        batching_max_publish_delay_ms=10,
        compression_type=pulsar.CompressionType.LZ4,
        # set send timeout to be infinity to prevent potential deadlock with consumer
        # that might happen when consumer is blocked due to unacked messages
        send_timeout_millis=0,
        properties=util.get_properties(util.getFullyQualifiedFunctionName(
                        self.instance_config.function_details.tenant,
                        self.instance_config.function_details.namespace,
                        self.instance_config.function_details.name),
                        self.instance_config.instance_id)
      )

  def setup_state(self):
    table_ns = "%s_%s" % (str(self.instance_config.function_details.tenant),
                          str(self.instance_config.function_details.namespace))
    table_ns = table_ns.replace("-", "_")
    table_name = str(self.instance_config.function_details.name)
    return state_context.create_state_context(self.state_storage_serviceurl, table_ns, table_name)

  def message_listener(self, serde, consumer, message):
    # increment number of received records from source
    self.stats.incr_total_received()
    item = InternalMessage(message, message.topic_name(), serde, consumer)
    self.queue.put(item, True)
    if self.atmost_once and self.auto_ack:
      consumer.acknowledge(message)

  def get_and_reset_metrics(self):
    # First get any user metrics
    metrics = self.get_metrics()
    self.reset_metrics()
    return metrics

  def reset_metrics(self):
    self.stats.reset()
    self.contextimpl.reset_metrics()

  def get_metrics(self):

    total_received =  self.stats.get_total_received()
    total_processed_successfully = self.stats.get_total_processed_successfully()
    total_user_exceptions = self.stats.get_total_user_exceptions()
    total_sys_exceptions = self.stats.get_total_sys_exceptions()
    avg_process_latency_ms = self.stats.get_avg_process_latency()
    last_invocation = self.stats.get_last_invocation()

    total_received_1min = self.stats.get_total_received_1min()
    total_processed_successfully_1min = self.stats.get_total_processed_successfully_1min()
    total_user_exceptions_1min = self.stats.get_total_user_exceptions_1min()
    total_sys_exceptions_1min = self.stats.get_total_sys_exceptions_1min()
    avg_process_latency_ms_1min = self.stats.get_avg_process_latency_1min()

    metrics_data = InstanceCommunication_pb2.MetricsData()
    # total metrics
    metrics_data.receivedTotal = int(total_received) if sys.version_info.major >= 3 else long(total_received)
    metrics_data.processedSuccessfullyTotal = int(total_processed_successfully) if sys.version_info.major >= 3 else long(total_processed_successfully)
    metrics_data.systemExceptionsTotal = int(total_sys_exceptions) if sys.version_info.major >= 3 else long(total_sys_exceptions)
    metrics_data.userExceptionsTotal = int(total_user_exceptions) if sys.version_info.major >= 3 else long(total_user_exceptions)
    metrics_data.avgProcessLatency = avg_process_latency_ms
    metrics_data.lastInvocation = int(last_invocation) if sys.version_info.major >= 3 else long(last_invocation)
    # 1min metrics
    metrics_data.receivedTotal_1min = int(total_received_1min) if sys.version_info.major >= 3 else long(total_received_1min)
    metrics_data.processedSuccessfullyTotal_1min = int(
      total_processed_successfully_1min) if sys.version_info.major >= 3 else long(total_processed_successfully_1min)
    metrics_data.systemExceptionsTotal_1min = int(total_sys_exceptions_1min) if sys.version_info.major >= 3 else long(
      total_sys_exceptions_1min)
    metrics_data.userExceptionsTotal_1min = int(total_user_exceptions_1min) if sys.version_info.major >= 3 else long(
      total_user_exceptions_1min)
    metrics_data.avgProcessLatency_1min = avg_process_latency_ms_1min

    # get any user metrics
    user_metrics = self.contextimpl.get_metrics()
    for metric_name, value in user_metrics.items():
      metrics_data.userMetrics[metric_name] = value

    return metrics_data

  def add_system_metrics(self, metric_name, value, metrics):
    metrics.metrics[metric_name].count = value
    metrics.metrics[metric_name].sum = value
    metrics.metrics[metric_name].min = 0
    metrics.metrics[metric_name].max = value

  def get_function_status(self):
    status = InstanceCommunication_pb2.FunctionStatus()
    status.running = True

    total_received = self.stats.get_total_received()
    total_processed_successfully = self.stats.get_total_processed_successfully()
    total_user_exceptions = self.stats.get_total_user_exceptions()
    total_sys_exceptions = self.stats.get_total_sys_exceptions()
    avg_process_latency_ms = self.stats.get_avg_process_latency()
    last_invocation = self.stats.get_last_invocation()

    status.numReceived = int(total_received) if sys.version_info.major >= 3 else long(total_received)
    status.numSuccessfullyProcessed = int(total_processed_successfully) if sys.version_info.major >= 3 else long(total_processed_successfully)
    status.numUserExceptions = int(total_user_exceptions) if sys.version_info.major >= 3 else long(total_user_exceptions)
    status.instanceId = self.instance_config.instance_id
    for ex, tm in self.stats.latest_user_exception:
      to_add = status.latestUserExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    status.numSystemExceptions = int(total_sys_exceptions) if sys.version_info.major >= 3 else long(total_sys_exceptions)
    for ex, tm in self.stats.latest_sys_exception:
      to_add = status.latestSystemExceptions.add()
      to_add.exceptionString = ex
      to_add.msSinceEpoch = tm
    status.averageLatency = avg_process_latency_ms
    status.lastInvocationTime = int(last_invocation) if sys.version_info.major >= 3 else long(last_invocation)
    return status

  def join(self):
    self.queue.put(InternalQuitMessage(True), True)
    self.execution_thread.join()
    self.close()

  def close(self):
    Log.info("Closing python instance...")
    if self.producer:
      self.producer.close()

    if self.consumers:
      for consumer in self.consumers.values():
        try:
          consumer.close()
        except:
          pass

    if self.pulsar_client:
      self.pulsar_client.close()
