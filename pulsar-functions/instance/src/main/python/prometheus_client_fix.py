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

from __future__ import unicode_literals

import threading

from prometheus_client import core
from prometheus_client.exposition import MetricsHandler

try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from SocketServer import ThreadingMixIn
    from urllib2 import build_opener, Request, HTTPHandler
    from urllib import quote_plus
    from urlparse import parse_qs, urlparse
except ImportError:
    # Python 3
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from socketserver import ThreadingMixIn
    from urllib.request import build_opener, Request, HTTPHandler
    from urllib.parse import quote_plus, parse_qs, urlparse


class _ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    """Thread per request HTTP server."""
    # Make worker threads "fire and forget". Beginning with Python 3.7 this
    # prevents a memory leak because ``ThreadingMixIn`` starts to gather all
    # non-daemon threads in a list in order to join on them at server close.
    # Enabling daemon threads virtually makes ``_ThreadingSimpleServer`` the
    # same as Python 3.7's ``ThreadingHTTPServer``.
    daemon_threads = True

def start_http_server(port, addr='', registry=core.REGISTRY):
    """Starts an HTTP server for prometheus metrics as a daemon thread"""
    CustomMetricsHandler = MetricsHandler.factory(registry)
    httpd = _ThreadingSimpleServer((addr, port), CustomMetricsHandler)
    t = threading.Thread(target=httpd.serve_forever)
    t.daemon = True
    t.start()