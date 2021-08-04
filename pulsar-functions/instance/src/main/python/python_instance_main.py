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
import json
import inspect
import threading

import pulsar

import Function_pb2
import log
import server
import python_instance
import util
# import prometheus_client
import prometheus_client_fix

from google.protobuf import json_format
from bookkeeper.kv.client import Client

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
  parser.add_argument('--metrics_port', required=True, help="Port metrics will be exposed on", type=int)
  parser.add_argument('--max_buffered_tuples', required=True, help='Maximum number of Buffered tuples')
  parser.add_argument('--logging_directory', required=True, help='Logging Directory')
  parser.add_argument('--logging_file', required=True, help='Log file name')
  parser.add_argument('--logging_config_file', required=True, help='Config file for logging')
  parser.add_argument('--expected_healthcheck_interval', required=True, help='Expected time in seconds between health checks', type=int)
  parser.add_argument('--secrets_provider', required=False, help='The classname of the secrets provider')
  parser.add_argument('--secrets_provider_config', required=False, help='The config that needs to be passed to secrets provider')
  parser.add_argument('--install_usercode_dependencies', required=False, help='For packaged python like wheel files, do we need to install all dependencies', type=bool)
  parser.add_argument('--dependency_repository', required=False, help='For packaged python like wheel files, which repository to pull the dependencies from')
  parser.add_argument('--extra_dependency_repository', required=False, help='For packaged python like wheel files, any extra repository to pull the dependencies from')
  parser.add_argument('--state_storage_serviceurl', required=False, help='Managed State Storage Service Url')
  parser.add_argument('--cluster_name', required=True, help='The name of the cluster this instance is running on')

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
      retval = os.system(cmd)
      if retval != 0:
        print("Could not install user depedencies")
        sys.exit(1)
    else:
      zpfile = zipfile.ZipFile(str(args.py), 'r')
      zpfile.extractall(os.path.dirname(str(args.py)))
    sys.path.insert(0, os.path.dirname(str(args.py)))
  elif os.path.splitext(str(args.py))[1] == '.zip':
    # Assumig zip file with format func.zip
    # extract to folder function
    # internal dir format
    # "func/src"
    # "func/requirements.txt"
    # "func/deps"
    # run pip install to target folder  deps folder
    zpfile = zipfile.ZipFile(str(args.py), 'r')
    zpfile.extractall(os.path.dirname(str(args.py)))
    basename = os.path.splitext(str(args.py))[0]

    deps_dir = os.path.join(os.path.dirname(str(args.py)), basename, "deps")

    if os.path.isdir(deps_dir) and os.listdir(deps_dir):
      # get all wheel files from deps directory
      wheel_file_list = [os.path.join(deps_dir, f) for f in os.listdir(deps_dir) if os.path.isfile(os.path.join(deps_dir, f)) and os.path.splitext(f)[1] =='.whl']
      cmd = "pip install -t %s --no-index --find-links %s %s" % (os.path.dirname(str(args.py)), deps_dir, " ".join(wheel_file_list))
      Log.debug("Install python dependencies via cmd: %s" % cmd)
      retval = os.system(cmd)
      if retval != 0:
        print("Could not install user depedencies specified by the zip file")
        sys.exit(1)
    # add python user src directory to path
    sys.path.insert(0, os.path.join(os.path.dirname(str(args.py)), basename, "src"))

  log_file = os.path.join(args.logging_directory,
                          util.getFullyQualifiedFunctionName(function_details.tenant, function_details.namespace, function_details.name),
                          "%s-%s.log" % (args.logging_file, args.instance_id))
  log.init_logger(logging.INFO, log_file, args.logging_config_file)

  Log.info("Starting Python instance with %s" % str(args))

  authentication = None
  use_tls = False
  tls_allow_insecure_connection = False
  tls_trust_cert_path = None
  hostname_verification_enabled = False
  if args.client_auth_plugin and args.client_auth_params:
      authentication = pulsar.Authentication(args.client_auth_plugin, args.client_auth_params)
  if args.use_tls == "true":
    use_tls = True
  if args.tls_allow_insecure_connection == "true":
    tls_allow_insecure_connection = True
  if args.tls_trust_cert_path:
     tls_trust_cert_path =  args.tls_trust_cert_path
  if args.hostname_verification_enabled == "true":
    hostname_verification_enabled = True
  pulsar_client = pulsar.Client(args.pulsar_serviceurl, authentication=authentication, operation_timeout_seconds=30,
                                io_threads=1, message_listener_threads=1, concurrent_lookup_requests=50000,
                                log_conf_file_path=None, use_tls=use_tls, tls_trust_certs_file_path=tls_trust_cert_path,
                                tls_allow_insecure_connection=tls_allow_insecure_connection,
                                tls_validate_hostname=hostname_verification_enabled)

  state_storage_serviceurl = None
  if args.state_storage_serviceurl is not None:
    state_storage_serviceurl = str(args.state_storage_serviceurl)

  secrets_provider = None
  if args.secrets_provider is not None:
    secrets_provider = util.import_class(os.path.dirname(inspect.getfile(inspect.currentframe())), str(args.secrets_provider))
  else:
    secrets_provider = util.import_class(os.path.dirname(inspect.getfile(inspect.currentframe())), "secretsprovider.ClearTextSecretsProvider")
  secrets_provider = secrets_provider()
  secrets_provider_config = None
  if args.secrets_provider_config is not None:
    args.secrets_provider_config = str(args.secrets_provider_config)
    if args.secrets_provider_config[0] == '\'':
      args.secrets_provider_config = args.secrets_provider_config[1:]
    if args.secrets_provider_config[-1] == '\'':
      args.secrets_provider_config = args.secrets_provider_config[:-1]
    secrets_provider_config = json.loads(str(args.secrets_provider_config))
  secrets_provider.init(secrets_provider_config)

  pyinstance = python_instance.PythonInstance(str(args.instance_id), str(args.function_id),
                                              str(args.function_version), function_details,
                                              int(args.max_buffered_tuples),
                                              int(args.expected_healthcheck_interval),
                                              str(args.py),
                                              pulsar_client,
                                              secrets_provider,
                                              args.cluster_name,
                                              state_storage_serviceurl)
  pyinstance.run()
  server_instance = server.serve(args.port, pyinstance)

  # Cannot use latest version of prometheus client because of thread leak
  # prometheus_client.start_http_server(args.metrics_port)
  # Use patched version of prometheus
  # Contains fix from https://github.com/prometheus/client_python/pull/356
  # This can be removed one the fix in is a official prometheus client release
  prometheus_client_fix.start_http_server(args.metrics_port)

  global to_run
  while to_run:
    time.sleep(1)

  pyinstance.join()
  # make sure to close all non-daemon threads before this!
  sys.exit(0)

if __name__ == '__main__':
  main()
