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

from distutils.command import build_ext


def get_version():
    # Get the pulsar version from pom.xml
    command = '''pushd ../.. > /dev/null
    mvn -q \\
         -Dexec.executable="echo" \\
         -Dexec.args='${project.version}' \\
         --non-recursive \\
         org.codehaus.mojo:exec-maven-plugin:1.3.1:exec;\\
     popd > /dev/null'''
    process = subprocess.Popen(['bash', '-c', command], stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        raise 'Failed to get version: ' + error
    return str(output.strip())


VERSION = get_version()


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


setup(
    name="pulsar-client",
    version=VERSION,
    py_modules=['pulsar'],
    cmdclass={'build_ext': my_build_ext},
    ext_modules=[Extension('_pulsar', [])],

    author="Pulsar Devs",
    author_email="dev@pulsar.incubator.apache.org",
    description="Apache Pulsar Python client library",
    license="Apache License v2.0",
    url="http://pulsar.incubator.apache.org/",
)
