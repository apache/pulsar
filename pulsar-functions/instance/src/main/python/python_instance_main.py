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

"""python_instance_main.py: The main for the Python Instance
"""
import argparse
import logging
import os
import sys
import signal
import time
import json

import pulsar

import Function_pb2
import log
import server
import python_instance
import util

to_run = True
Log = log.Log

def atexit_function(signo, _frame):
  global to_run
  Log.info("Interrupted by %d, shutting down" % signo)
  to_run = False

def main():
  # Setup signal handlers
  signal.signal(signal.SIGTERM, atexit_function)
  signal.signal(signal.SIGHUP, atexit_function)
  signal.signal(signal.SIGINT, atexit_function)

  parser = argparse.ArgumentParser(description='Pulsar Functions Python Instance')
  parser.add_argument('--function_classname', required=True, help='Function Class Name')
  parser.add_argument('--py', required=True, help='Full Path of Function Code File')
  parser.add_argument('--name', required=True, help='Function Name')
  parser.add_argument('--tenant', required=True, help='Tenant Name')
  parser.add_argument('--namespace', required=True, help='Namespace name')
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
  parser.add_argument('--auto_ack', required=True, help='Enable Autoacking?')
  parser.add_argument('--log_topic', required=False, help='Topic to send Log Messages')
  parser.add_argument('--source_subscription_type', required=True, help='Subscription Type')
  parser.add_argument('--source_topics_serde_classname', required=True, help='A mapping of Input topics to SerDe')
  parser.add_argument('--topics_pattern', required=False, help='TopicsPattern to consume from list of topics under a namespace that match the pattern (not supported)')
  parser.add_argument('--source_timeout_ms', required=False, help='Source message timeout in milliseconds')
  parser.add_argument('--sink_topic', required=False, help='Sink Topic')
  parser.add_argument('--sink_serde_classname', required=False, help='Sink SerDe classname')

  args = parser.parse_args()
  log_file = os.path.join(args.logging_directory,
                          util.getFullyQualifiedFunctionName(args.tenant, args.namespace, args.name),
                          "%s-%s.log" % (args.logging_file, args.instance_id))
  log.init_rotating_logger(level=logging.INFO, logfile=log_file,
                           max_files=5, max_bytes=10 * 1024 * 1024)

  Log.info("Starting Python instance with %s" % str(args))

  function_details = Function_pb2.FunctionDetails()
  function_details.tenant = args.tenant
  function_details.namespace = args.namespace
  function_details.name = args.name
  function_details.className = args.function_classname

  if args.topics_pattern:
    raise ValueError('topics_pattern is not supported by python client') 
  sourceSpec = Function_pb2.SourceSpec()
  sourceSpec.subscriptionType = Function_pb2.SubscriptionType.Value(args.source_subscription_type)
  try:
    source_topics_serde_classname_dict = json.loads(args.source_topics_serde_classname)
  except ValueError:
    log.critical("Cannot decode source_topics_serde_classname.  This argument must be specifed as a JSON")
    sys.exit(1)
  if not source_topics_serde_classname_dict:
    log.critical("source_topics_serde_classname cannot be empty")
  for topics, serde_classname in source_topics_serde_classname_dict.items():
    sourceSpec.topicsToSerDeClassName[topics] = serde_classname
  if args.source_timeout_ms:
    sourceSpec.timeoutMs = long(args.source_timeout_ms)
  function_details.source.MergeFrom(sourceSpec)

  sinkSpec = Function_pb2.SinkSpec()
  if args.sink_topic != None and len(args.sink_topic) != 0:
    sinkSpec.topic = args.sink_topic
  if args.sink_serde_classname != None and len(args.sink_serde_classname) != 0:
    sinkSpec.serDeClassName = args.sink_serde_classname
  function_details.sink.MergeFrom(sinkSpec)

  function_details.processingGuarantees = Function_pb2.ProcessingGuarantees.Value(args.processing_guarantees)
  if args.auto_ack == "true":
    function_details.autoAck = True
  else:
    function_details.autoAck = False
  if args.user_config != None and len(args.user_config) != 0:
    function_details.userConfig = args.user_config

  pulsar_client = pulsar.Client(args.pulsar_serviceurl)
  pyinstance = python_instance.PythonInstance(str(args.instance_id), str(args.function_id),
                                              str(args.function_version), function_details,
                                              int(args.max_buffered_tuples), str(args.py),
                                              args.log_topic, pulsar_client)
  pyinstance.run()
  server_instance = server.serve(args.port, pyinstance)

  global to_run
  while to_run:
    time.sleep(1)

  pyinstance.join()
  sys.exit(1)

if __name__ == '__main__':
  main()
