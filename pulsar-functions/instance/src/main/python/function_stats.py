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

import traceback
import time
import util
import sys

from prometheus_client import Counter, Summary, Gauge
from ratelimit import limits, RateLimitException

# We keep track of the following metrics
class Stats(object):
  metrics_label_names = ['tenant', 'namespace', 'name', 'instance_id', 'cluster', 'fqfn']

  exception_metrics_label_names = metrics_label_names + ['error']

  PULSAR_FUNCTION_METRICS_PREFIX = "pulsar_function_"
  USER_METRIC_PREFIX = "user_metric_"

  TOTAL_SUCCESSFULLY_PROCESSED = 'processed_successfully_total'
  TOTAL_SYSTEM_EXCEPTIONS = 'system_exceptions_total'
  TOTAL_USER_EXCEPTIONS = 'user_exceptions_total'
  PROCESS_LATENCY_MS = 'process_latency_ms'
  LAST_INVOCATION = 'last_invocation'
  TOTAL_RECEIVED = 'received_total'

  TOTAL_SUCCESSFULLY_PROCESSED_1min = 'processed_successfully_1min_total'
  TOTAL_SYSTEM_EXCEPTIONS_1min = 'system_exceptions_1min_total'
  TOTAL_USER_EXCEPTIONS_1min = 'user_exceptions_1min_total'
  PROCESS_LATENCY_MS_1min = 'process_latency_ms_1min'
  TOTAL_RECEIVED_1min = 'received_1min_total'

  # Declare Prometheus
  stat_total_processed_successfully = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED,
                                              'Total number of messages processed successfully.', metrics_label_names)
  stat_total_sys_exceptions = Counter(PULSAR_FUNCTION_METRICS_PREFIX+ TOTAL_SYSTEM_EXCEPTIONS, 'Total number of system exceptions.',
                                      metrics_label_names)
  stat_total_user_exceptions = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS, 'Total number of user exceptions.',
                                       metrics_label_names)

  stat_process_latency_ms = Summary(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS, 'Process latency in milliseconds.', metrics_label_names)

  stat_last_invocation = Gauge(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION, 'The timestamp of the last invocation of the function.', metrics_label_names)

  stat_total_received = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED, 'Total number of messages received from source.', metrics_label_names)

  # 1min windowed metrics
  stat_total_processed_successfully_1min = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED_1min,
                                              'Total number of messages processed successfully in the last 1 minute.', metrics_label_names)
  stat_total_sys_exceptions_1min = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SYSTEM_EXCEPTIONS_1min,
                                      'Total number of system exceptions in the last 1 minute.',
                                      metrics_label_names)
  stat_total_user_exceptions_1min = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS_1min,
                                       'Total number of user exceptions in the last 1 minute.',
                                       metrics_label_names)

  stat_process_latency_ms_1min = Summary(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min,
                                    'Process latency in milliseconds in the last 1 minute.', metrics_label_names)

  stat_total_received_1min = Counter(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED_1min,
                                'Total number of messages received from source in the last 1 minute.', metrics_label_names)

  # exceptions
  user_exceptions = Gauge(PULSAR_FUNCTION_METRICS_PREFIX + 'user_exception', 'Exception from user code.', exception_metrics_label_names)

  system_exceptions = Gauge(PULSAR_FUNCTION_METRICS_PREFIX + 'system_exception', 'Exception from system code.', exception_metrics_label_names)

  latest_user_exception = []
  latest_sys_exception = []

  def __init__(self, metrics_labels):
    self.metrics_labels = metrics_labels
    self.process_start_time = None

    # as optimization
    self._stat_total_processed_successfully = self.stat_total_processed_successfully.labels(*self.metrics_labels)
    self._stat_total_sys_exceptions = self.stat_total_sys_exceptions.labels(*self.metrics_labels)
    self._stat_total_user_exceptions = self.stat_total_user_exceptions.labels(*self.metrics_labels)
    self._stat_process_latency_ms = self.stat_process_latency_ms.labels(*self.metrics_labels)
    self._stat_last_invocation = self.stat_last_invocation.labels(*self.metrics_labels)
    self._stat_total_received = self.stat_total_received.labels(*self.metrics_labels)
    self._stat_total_processed_successfully_1min = self.stat_total_processed_successfully_1min.labels(*self.metrics_labels)
    self._stat_total_sys_exceptions_1min = self.stat_total_sys_exceptions_1min.labels(*self.metrics_labels)
    self._stat_total_user_exceptions_1min = self.stat_total_user_exceptions_1min.labels(*self.metrics_labels)
    self._stat_process_latency_ms_1min = self.stat_process_latency_ms_1min.labels(*self.metrics_labels)
    self._stat_total_received_1min = self.stat_total_received_1min.labels(*self.metrics_labels)

    # start time for windowed metrics
    util.FixedTimer(60, self.reset, name="windowed-metrics-timer").start()

  def get_total_received(self):
    return self._stat_total_received._value.get()

  def get_total_processed_successfully(self):
    return self._stat_total_processed_successfully._value.get()

  def get_total_sys_exceptions(self):
    return self._stat_total_sys_exceptions._value.get()

  def get_total_user_exceptions(self):
    return self._stat_total_user_exceptions._value.get()

  def get_avg_process_latency(self):
    process_latency_ms_count = self._stat_process_latency_ms._count.get()
    process_latency_ms_sum = self._stat_process_latency_ms._sum.get()
    return 0.0 \
      if process_latency_ms_count <= 0.0 \
      else process_latency_ms_sum / process_latency_ms_count

  def get_total_processed_successfully_1min(self):
    return self._stat_total_processed_successfully_1min._value.get()

  def get_total_sys_exceptions_1min(self):
    return self._stat_total_sys_exceptions_1min._value.get()

  def get_total_user_exceptions_1min(self):
    return self._stat_total_user_exceptions_1min._value.get()

  def get_total_received_1min(self):
    return self._stat_total_received_1min._value.get()

  def get_avg_process_latency_1min(self):
    process_latency_ms_count = self._stat_process_latency_ms_1min._count.get()
    process_latency_ms_sum = self._stat_process_latency_ms_1min._sum.get()
    return 0.0 \
      if process_latency_ms_count <= 0.0 \
      else process_latency_ms_sum / process_latency_ms_count

  def get_last_invocation(self):
    return self._stat_last_invocation._value.get()

  def incr_total_processed_successfully(self):
    self._stat_total_processed_successfully.inc()
    self._stat_total_processed_successfully_1min.inc()

  def incr_total_sys_exceptions(self, exception):
    self._stat_total_sys_exceptions.inc()
    self._stat_total_sys_exceptions_1min.inc()
    self.add_sys_exception(exception)

  def incr_total_user_exceptions(self, exception):
    self._stat_total_user_exceptions.inc()
    self._stat_total_user_exceptions_1min.inc()
    self.add_user_exception(exception)

  def incr_total_received(self):
    self._stat_total_received.inc()
    self._stat_total_received_1min.inc()

  def process_time_start(self):
    self.process_start_time = time.time()

  def process_time_end(self):
    if self.process_start_time:
      duration = (time.time() - self.process_start_time) * 1000.0
      self._stat_process_latency_ms.observe(duration)
      self._stat_process_latency_ms_1min.observe(duration)

  def set_last_invocation(self, time):
    self._stat_last_invocation.set(time * 1000.0)

  def add_user_exception(self, exception):
    error = traceback.format_exc()
    ts = int(time.time() * 1000) if sys.version_info.major >= 3 else long(time.time() * 1000)
    self.latest_user_exception.append((error, ts))
    if len(self.latest_user_exception) > 10:
      self.latest_user_exception.pop(0)

    # report exception via prometheus
    try:
      self.report_user_exception_prometheus(exception)
    except RateLimitException:
      pass

  @limits(calls=5, period=60)
  def report_user_exception_prometheus(self, exception):
    exception_metric_labels = self.metrics_labels + [str(exception)]
    self.user_exceptions.labels(*exception_metric_labels).set(1.0)

  def add_sys_exception(self, exception):
    error = traceback.format_exc()
    ts = int(time.time() * 1000) if sys.version_info.major >= 3 else long(time.time() * 1000)
    self.latest_sys_exception.append((error, ts))
    if len(self.latest_sys_exception) > 10:
      self.latest_sys_exception.pop(0)

    # report exception via prometheus
    try:
      self.report_system_exception_prometheus(exception)
    except RateLimitException:
      pass

  @limits(calls=5, period=60)
  def report_system_exception_prometheus(self, exception):
    exception_metric_labels = self.metrics_labels + [str(exception)]
    self.system_exceptions.labels(*exception_metric_labels).set(1.0)

  def reset(self):
    self._stat_total_processed_successfully_1min._value.set(0.0)
    self._stat_total_user_exceptions_1min._value.set(0.0)
    self._stat_total_sys_exceptions_1min._value.set(0.0)
    self._stat_process_latency_ms_1min._sum.set(0.0)
    self._stat_process_latency_ms_1min._count.set(0.0)
    self._stat_total_received_1min._value.set(0.0)
