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

import sys, getopt, time, logging
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.retry import KazooRetry

logging.getLogger('kazoo.client').addHandler(logging.StreamHandler())

def usage():
    print >> sys.stderr, "\n%s -z <zookeeper> -p <path> [-w|-c|-e]" % (sys.argv[0])
    print >> sys.stderr, "\nWait for, or create znode"
    print >> sys.stderr, "\n-z Specify zookeeper connect string"
    print >> sys.stderr, "\n-p Znode path to watch or create"
    print >> sys.stderr, "\n-w Watch for path creation"
    print >> sys.stderr, "\n-c Create path"
    print >> sys.stderr, "\n-e Check if znode exists"

try:
    opts, args = getopt.getopt(sys.argv[1:], "z:p:cweh")
except getopt.GetoptError as err:
    print >> sys.stderr, str(err)
    usage()
    sys.exit(2)

zookeeper = None
znode = None
create = False
watch = False
exists = False

for o, a in opts:
    if o in ("-h"):
        usage()
        sys.exit()
    elif o in ("-z"):
        zookeeper = a
    elif o in ("-p"):
        znode = a
    elif o in ("-w"):
        watch = True
    elif o in ("-c"):
        create = True
    elif o in ("-e"):
        exists = True
    else:
        assert False, "unhandled option"
        usage()
        sys.exit(2)

if not zookeeper:
    print >> sys.stderr, "Zookeeper must be specified"
    usage()
    sys.exit(3)

if not znode:
    print >> sys.stderr, "Znode must be specified"
    usage()
    sys.exit(4)

if (not watch and not create and not exists):
    print >> sys.stderr, "Exactly one of watch (-w), create (-c) or exists (-e) must be specified"
    usage()
    sys.exit(5)

while True:
    zk = KazooClient(hosts=zookeeper, timeout=1, connection_retry=KazooRetry(max_tries=-1))
    try:
        zk.start()
        if create:
            try:
                zk.create(znode)
            except NodeExistsError:
                pass
            sys.exit(0)
        elif watch:
            while not zk.exists(znode):
                print "Waiting for %s" % znode
                time.sleep(1)
            sys.exit(0)
        elif exists:
            if zk.exists(znode):
                sys.exit(0)
            else:
                sys.exit(-1)
    finally:
        zk.stop()
        zk.close()
