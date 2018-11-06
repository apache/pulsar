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

''' log.py '''
import logging
import logging.config
import logging.handlers
import os
import pulsar

# Create the logger
# pylint: disable=invalid-name
Log = None

# time formatter - date - time - UTC offset
# e.g. "08/16/1988 21:30:00 +1030"
# see time formatter documentation for more
date_format = "%Y-%m-%d %H:%M:%S %z"

class LogTopicHandler(logging.Handler):
  def __init__(self, topic_name, pulsar_client):
    logging.Handler.__init__(self)
    Log.info("Setting up producer for log topic %s" % topic_name)
    self.producer = pulsar_client.create_producer(
      str(topic_name),
      block_if_queue_full=True,
      batching_enabled=True,
      batching_max_publish_delay_ms=100,
      compression_type=pulsar._pulsar.CompressionType.LZ4)

  def emit(self, record):
    msg = self.format(record)
    self.producer.send_async(str(msg).encode('utf-8'), None)

def remove_all_handlers():
  retval = None
  for handler in Log.handlers:
    Log.handlers.remove(handler)
    retval = handler
  return retval

def add_handler(stream_handler):
  log_format = "[%(asctime)s] [%(levelname)s]: %(message)s"
  formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
  stream_handler.setFormatter(formatter)
  Log.addHandler(stream_handler)

def init_logger(level, logfile, logging_config_file):
  global Log
  # get log file location for function instance
  os.environ['LOG_FILE'] = logfile
  logging.config.fileConfig(logging_config_file)
  Log = logging.getLogger()
  Log.setLevel(level)
