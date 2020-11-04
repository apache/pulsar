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
import errno
import pulsar
import sys

# Create the logger
# pylint: disable=invalid-name
logging.basicConfig()
Log = logging.getLogger()

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

def mkdir_p(path):
  try:
    os.makedirs(path, exist_ok=True)  # Python>3.2
  except TypeError:
    try:
      os.makedirs(path)
    except OSError as exc: # Python >2.5
      if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
      else: raise

# logging handler that is RotatingFileHandler but creates path to log file for you
# if it doesn't exist
class CreatePathRotatingFileHandler(logging.handlers.RotatingFileHandler):
  def __init__(self, filename, mode='a', maxBytes=10 * 1024 * 1024, backupCount=5, encoding=None, delay=0):
    mkdir_p(os.path.dirname(filename))
    logging.handlers.RotatingFileHandler.__init__(self, filename, mode=mode, maxBytes=maxBytes, backupCount=backupCount, encoding=encoding, delay=delay)

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

  # set print to redirect to logger
  class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''

    def write(self, buf):
      for line in buf.rstrip().splitlines():
        self.logger.log(self.log_level, line.rstrip())

    def flush(self):
      pass

  sl = StreamToLogger(Log, logging.INFO)
  sys.stdout = sl

  sl = StreamToLogger(Log, logging.ERROR)
  sys.stderr = sl
