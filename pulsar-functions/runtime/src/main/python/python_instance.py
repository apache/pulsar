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
"""python_instance.py: Python Instance for running python functions
"""
import argparse
import logging
import os
import log

import server

Log = log.Log

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
  parser.add_argument('--pulsar_serviceurl', required=True, help='Pulsar Service Url')
  parser.add_argument('--port', required=True, help='Instance Port', type=int)
  parser.add_argument('--max_buffered_tuples', required=True, help='Maximum number of Buffered tuples')
  parser.add_argument('--user_config', required=False, help='User Config')

  args = parser.parse_args()
  log_dir = os.environ['LOGGING_DIRECTORY']
  log_filename = os.environ['LOGGING_FILE']
  log_file = os.path.join(log_dir, log_filename + ".log.0")
  log.init_rotating_logger(level=logging.INFO, logfile=log_file,
                           max_files=5, max_bytes=10 * 1024 * 1024)

  Log.info("Starting Python instance with %s" % str(args))
  server.serve(args.port)

if __name__ == '__main__':
  main()