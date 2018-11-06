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

from prometheus_client import Counter, Summary, Gauge

# We keep track of the following metrics
class Stats(object):
  metrics_label_names = ['tenant', 'namespace', 'name', 'instance_id', 'cluster']

  TOTAL_PROCESSED = 'pulsar_function_processed_total'
  TOTAL_SUCCESSFULLY_PROCESSED = 'pulsar_function_processed_successfully_total'
  TOTAL_SYSTEM_EXCEPTIONS = 'pulsar_function_system_exceptions_total'
  TOTAL_USER_EXCEPTIONS = 'pulsar_function_user_exceptions_total'
  PROCESS_LATENCY_MS = 'pulsar_function_process_latency_ms'
  LAST_INVOCATION = 'pulsar_function_last_invocation'
  TOTAL_RECEIVED = 'pulsar_function_received_total'

  # Declare Prometheus
  stat_total_processed = Counter(TOTAL_PROCESSED, 'Total number of messages processed.', metrics_label_names)
  stat_total_processed_successfully = Counter(TOTAL_SUCCESSFULLY_PROCESSED,
                                              'Total number of messages processed successfully.', metrics_label_names)
  stat_total_sys_exceptions = Counter(TOTAL_SYSTEM_EXCEPTIONS, 'Total number of system exceptions.',
                                      metrics_label_names)
  stat_total_user_exceptions = Counter(TOTAL_USER_EXCEPTIONS, 'Total number of user exceptions.',
                                       metrics_label_names)

  stat_process_latency_ms = Summary(PROCESS_LATENCY_MS, 'Process latency in milliseconds.', metrics_label_names)

  stat_last_invocation = Gauge(LAST_INVOCATION, 'The timestamp of the last invocation of the function.', metrics_label_names)

  stat_total_received = Counter(TOTAL_RECEIVED, 'Total number of messages received from source.', metrics_label_names)

  latest_user_exception = []
  latest_sys_exception = []

  def add_user_exception(self):
    self.latest_sys_exception.append((traceback.format_exc(), int(time.time() * 1000)))
    if len(self.latest_sys_exception) > 10:
      self.latest_sys_exception.pop(0)

  def add_sys_exception(self):
    self.latest_sys_exception.append((traceback.format_exc(), int(time.time() * 1000)))
    if len(self.latest_sys_exception) > 10:
      self.latest_sys_exception.pop(0)

  def reset(self, metrics_labels):
    self.latest_user_exception = []
    self.latest_sys_exception = []
    self.stat_total_processed.labels(*metrics_labels)._value.set(0.0)
    self.stat_total_processed_successfully.labels(*metrics_labels)._value.set(0.0)
    self.stat_total_user_exceptions.labels(*metrics_labels)._value.set(0.0)
    self.stat_total_sys_exceptions.labels(*metrics_labels)._value.set(0.0)
    self.stat_process_latency_ms.labels(*metrics_labels)._sum.set(0.0)
    self.stat_process_latency_ms.labels(*metrics_labels)._count.set(0.0)
    self.stat_last_invocation.labels(*metrics_labels).set(0.0)
    self.stat_total_received.labels(*metrics_labels)._value.set(0.0)