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

PYTHON_VERSIONS=(
   '2.7  cp27-cp27mu manylinux1    x86_64'
   '2.7  cp27-cp27m  manylinux1    x86_64'
   '3.5  cp35-cp35m  manylinux1    x86_64'
   '3.6  cp36-cp36m  manylinux2014 x86_64'
   '3.7  cp37-cp37m  manylinux2014 x86_64'
   '3.8  cp38-cp38   manylinux2014 x86_64'
   '3.9  cp39-cp39   manylinux2014 x86_64'
   '3.10 cp310-cp310 manylinux2014 x86_64'
   '3.7  cp37-cp37m  manylinux2014 aarch64'
   '3.8  cp38-cp38   manylinux2014 aarch64'
   '3.9  cp39-cp39   manylinux2014 aarch64'
   '3.10 cp310-cp310 manylinux2014 aarch64'

   # Alpine compatible wheels
   '3.7  cp37-cp37m  manylinux_musl aarch64'
   '3.8  cp38-cp38   manylinux_musl aarch64'
   '3.9  cp39-cp39   manylinux_musl aarch64'
   '3.10 cp310-cp310 manylinux_musl aarch64'

   '3.7  cp37-cp37m  manylinux_musl x86_64'
   '3.8  cp38-cp38   manylinux_musl x86_64'
   '3.9  cp39-cp39   manylinux_musl x86_64'
   '3.10 cp310-cp310 manylinux_musl x86_64'
)
