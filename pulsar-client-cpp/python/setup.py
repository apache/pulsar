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

from setuptools import setup
from distutils.core import Extension
import subprocess
import sys

from distutils.command import build_ext

import xml.etree.ElementTree as ET
from os.path import dirname, realpath, join

def get_version():
    # Get the pulsar version from pom.xml
    TOP_LEVEL_PATH = dirname(dirname(dirname(realpath(__file__))))
    POM_PATH = join(TOP_LEVEL_PATH, 'pom.xml')
    root = ET.XML(open(POM_PATH).read())
    version = root.find('{http://maven.apache.org/POM/4.0.0}version').text.strip()

    # Strip the '-incubating' suffix, since it prevents the packages
    # from being uploaded into PyPI
    return version.split('-')[0]


VERSION = get_version()

if sys.version_info[0] == 2:
    PY2 = True
else:
    PY2 = False

# This is a workaround to have setuptools to include
# the already compiled _pulsar.so library
class my_build_ext(build_ext.build_ext):
    def build_extension(self, ext):
        import shutil
        import os.path

        try:
            os.makedirs(os.path.dirname(self.get_ext_fullpath(ext.name)))
        except OSError as e:
            if e.errno != 17:  # already exists
                raise
        shutil.copyfile('_pulsar.so', self.get_ext_fullpath(ext.name))


dependencies = [
    'fastavro',
    'grpcio',
    'protobuf>=3.6.1',
    'six',
    'certifi',

    # functions dependencies
    "apache-bookkeeper-client>=4.9.2",
    "prometheus_client",
    "ratelimit"
]

if PY2:
    # Python 2 compat dependencies
    dependencies += ['enum34']

setup(
    name="pulsar-client",
    version=VERSION,
    packages=['pulsar', 'pulsar.schema', 'pulsar.functions'],
    cmdclass={'build_ext': my_build_ext},
    ext_modules=[Extension('_pulsar', [])],

    author="Pulsar Devs",
    author_email="dev@pulsar.apache.org",
    description="Apache Pulsar Python client library",
    license="Apache License v2.0",
    url="https://pulsar.apache.org/",
    install_requires=dependencies,
)
