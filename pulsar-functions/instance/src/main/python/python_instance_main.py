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
import zipfile

import pulsar

import Function_pb2
import log
import server
import python_instance
import util
from google.protobuf import json_format

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
  parser.add_argument('--function_details', required=True, help='Function Details Json String')
  parser.add_argument('--py', required=True, help='Full Path of Function Code File')
  parser.add_argument('--instance_id', required=True, help='Instance Id')
  parser.add_argument('--function_id', required=True, help='Function Id')
  parser.add_argument('--function_version', required=True, help='Function Version')
  parser.add_argument('--pulsar_serviceurl', required=True, help='Pulsar Service Url')
  parser.add_argument('--client_auth_plugin', required=False, help='Client authentication plugin')
  parser.add_argument('--client_auth_params', required=False, help='Client authentication params')
  parser.add_argument('--use_tls', required=False, help='Use tls')
  parser.add_argument('--tls_allow_insecure_connection', required=False, help='Tls allow insecure connection')
  parser.add_argument('--hostname_verification_enabled', required=False, help='Enable hostname verification')
  parser.add_argument('--tls_trust_cert_path', required=False, help='Tls trust cert file path')
  parser.add_argument('--port', required=True, help='Instance Port', type=int)
  parser.add_argument('--max_buffered_tuples', required=True, help='Maximum number of Buffered tuples')
  parser.add_argument('--logging_directory', required=True, help='Logging Directory')
  parser.add_argument('--logging_file', required=True, help='Log file name')
  parser.add_argument('--logging_config_file', required=True, help='Config file for logging')
  parser.add_argument('--expected_healthcheck_interval', required=True, help='Expected time in seconds between health checks', type=int)
  parser.add_argument('--install_usercode_dependencies', required=False, help='For packaged python like wheel files, do we need to install all dependencies', type=bool)
  parser.add_argument('--dependency_repository', required=False, help='For packaged python like wheel files, which repository to pull the dependencies from')
  parser.add_argument('--extra_dependency_repository', required=False, help='For packaged python like wheel files, any extra repository to pull the dependencies from')


  args = parser.parse_args()
  function_details = Function_pb2.FunctionDetails()
  args.function_details = str(args.function_details)
  if args.function_details[0] == '\'':
    args.function_details = args.function_details[1:]
  if args.function_details[-1] == '\'':
    args.function_details = args.function_details[:-1]
  json_format.Parse(args.function_details, function_details)

  if os.path.splitext(str(args.py))[1] == '.whl':
    if args.install_usercode_dependencies:
      cmd = "pip install -t %s" % os.path.dirname(str(args.py))
      if args.dependency_repository:
        cmd = cmd + " -i %s" % str(args.dependency_repository)
      if args.extra_dependency_repository:
        cmd = cmd + " --extra-index-url %s" % str(args.extra_dependency_repository)
      cmd = cmd + " %s" % str(args.py)
      os.system(cmd)
    else:
      zpfile = zipfile.ZipFile(str(args.py), 'r')
      zpfile.extractall(os.path.dirname(str(args.py)))
    sys.path.insert(0, os.path.dirname(str(args.py)))

  log_file = os.path.join(args.logging_directory,
                          util.getFullyQualifiedFunctionName(function_details.tenant, function_details.namespace, function_details.name),
                          "%s-%s.log" % (args.logging_file, args.instance_id))
  log.init_logger(logging.INFO, log_file, args.logging_config_file)

  Log.info("Starting Python instance with %s" % str(args))

  authentication = None
  use_tls = False
  tls_allow_insecure_connection = False
  tls_trust_cert_path = None
  if args.client_auth_plugin and args.client_auth_params:
      authentication = pulsar.Authentication(args.client_auth_plugin, args.client_auth_params)
  if args.use_tls == "true":
    use_tls = True
  if args.tls_allow_insecure_connection == "true":
    tls_allow_insecure_connection = True
  if args.tls_trust_cert_path:
     tls_trust_cert_path =  args.tls_trust_cert_path
  pulsar_client = pulsar.Client(args.pulsar_serviceurl, authentication, 30, 1, 1, 50000, None, use_tls, tls_trust_cert_path, tls_allow_insecure_connection)
  pyinstance = python_instance.PythonInstance(str(args.instance_id), str(args.function_id),
                                              str(args.function_version), function_details,
                                              int(args.max_buffered_tuples),
                                              int(args.expected_healthcheck_interval),
                                              str(args.py), pulsar_client)
  pyinstance.run()
  server_instance = server.serve(args.port, pyinstance)

  global to_run
  while to_run:
    time.sleep(1)

  pyinstance.join()
  sys.exit(1)

if __name__ == '__main__':
  main()
