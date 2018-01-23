#!/usr/bin/env python
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
"""python_instance_main.py: The main for the Python Instance
"""
import argparse
import logging
import os
import sys
from collections import namedtuple

import pulsar

import Function_pb2
import log
import server
import python_instance

Log = log.Log
LimitsConfig = namedtuple('LimitsConfig', 'max_time_ms max_memory_mb max_cpu max_buffered_tuples')

def main():
  parser = argparse.ArgumentParser(description='Heron Python Instance')
  parser.add_argument('--function_classname', required=True, help='Function Class Name')
  parser.add_argument('--py', required=True, help='Full Path of Function Code File')
  parser.add_argument('--name', required=True, help='Function Name')
  parser.add_argument('--tenant', required=True, help='Tenant Name')
  parser.add_argument('--namespace', required=True, help='Namespace name')
  parser.add_argument('--source_topics', required=True, help='Source Topics')
  parser.add_argument('--input_serde_classnames', required=True, help='Input Serde Classnames')
  parser.add_argument('--sink_topic', required=False, help='Sink Topic')
  parser.add_argument('--output_serde_classname', required=False, help='Output Serde Classnames')
  parser.add_argument('--instance_id', required=True, help='Instance Id')
  parser.add_argument('--function_id', required=True, help='Function Id')
  parser.add_argument('--function_version', required=True, help='Function Version')
  parser.add_argument('--processing_guarantees', required=True, help='Processing Guarantees')
  parser.add_argument('--pulsar_serviceurl', required=True, help='Pulsar Service Url')
  parser.add_argument('--port', required=True, help='Instance Port', type=int)
  parser.add_argument('--max_buffered_tuples', required=True, help='Maximum number of Buffered tuples')
  parser.add_argument('--user_config', required=False, help='User Config')
  parser.add_argument('--logging_directory', required=True, help='Logging Directory')
  parser.add_argument('--logging_file', required=True, help='Log file name')

  args = parser.parse_args()
  log_file = os.path.join(args.logging_directory, args.logging_file + ".log.0")
  log.init_rotating_logger(level=logging.INFO, logfile=log_file,
                           max_files=5, max_bytes=10 * 1024 * 1024)

  Log.info("Starting Python instance with %s" % str(args))

  function_config = Function_pb2.FunctionConfig()
  function_config.tenant = args.tenant
  function_config.namespace = args.namespace
  function_config.name = args.name
  function_config.className = args.function_classname
  source_topics = args.source_topics.split(",")
  source_serde = args.input_serde_classnames.split(",")
  if len(source_topics) != len(source_serde):
    Log.critical("SourceTopics and Input Serde should match")
    sys.exit(1)
  for i in xrange(len(source_topics)):
    function_config.inputs[source_topics[i]] = source_serde[i]
  if args.sink_topic != None and len(args.sink_topic) != 0:
    function_config.sinkTopic = args.sink_topic
  if args.output_serde_classname != None and len(args.output_serde_classname) != 0:
    function_config.outputSerdeClassName = args.output_serde_classname
  function_config.processingGuarantees = Function_pb2.FunctionConfig.ProcessingGuarantees.Value(args.processing_guarantees)
  if args.user_config != None and len(args.user_config) != 0:
    user_config = args.user_config.split(",")
    for config in user_config:
      (key, value) = config.split(":")
      function_config.userConfig[key] = value

  # TODO(sanjeev):- Implement limits
  limits = LimitsConfig(-1, -1, -1, args.max_buffered_tuples)

  pulsar_client = pulsar.Client(args.pulsar_serviceurl)
  pyinstance = python_instance.PythonInstance(str(args.instance_id), str(args.function_id),
                                              str(args.function_version), function_config,
                                              limits, str(args.py), pulsar_client)
  pyinstance.run()
  server.serve(args.port, pyinstance)
  sys.exit(1)

if __name__ == '__main__':
  main()