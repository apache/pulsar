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
from distutils.util import strtobool
from os import environ
import subprocess

from distutils.command import build_ext

import xml.etree.ElementTree as ET
from os.path import dirname, realpath, join

def get_version():
    use_full_pom_name = strtobool(environ.get('USE_FULL_POM_NAME', 'False'))

    # Get the pulsar version from pom.xml
    TOP_LEVEL_PATH = dirname(dirname(dirname(realpath(__file__))))
    POM_PATH = join(TOP_LEVEL_PATH, 'pom.xml')
    root = ET.XML(open(POM_PATH).read())
    version = root.find('{http://maven.apache.org/POM/4.0.0}version').text.strip()

    if use_full_pom_name:
        return version
    else:
        # Strip the '-incubating' suffix, since it prevents the packages
        # from being uploaded into PyPI
        return version.split('-')[0]


def get_name():
    postfix = environ.get('NAME_POSTFIX', '')
    base = 'pulsar-client'
    return base + postfix

VERSION = get_version()
NAME = get_name()

print(VERSION)
print(NAME)

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

# Core Client dependencies
dependencies = [
    'six',
    'certifi',
    'enum34>=1.1.9; python_version < "3.4"'
]

extras_require = {}

# functions dependencies
extras_require["functions"] = sorted(
    {
      "protobuf>=3.6.1,<=3.20.*",
      "grpcio>=1.60",
      "apache-bookkeeper-client>=4.9.2",
      "prometheus_client",
      "ratelimit"
    }
)

# avro dependencies
extras_require["avro"] = sorted(
    {
      "fastavro==0.24.0"
    }
)

# all dependencies
extras_require["all"] = sorted(set(sum(extras_require.values(), [])))

setup(
    name=NAME,
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
    extras_require=extras_require,
)
