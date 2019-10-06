/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#pragma once

#include <boost/date_time/local_time/local_time.hpp>

#include <pulsar/defines.h>

namespace pulsar {

using namespace boost::posix_time;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;

class PULSAR_PUBLIC TimeUtils {
   public:
    static ptime now();
    static int64_t currentTimeMillis();
};
}  // namespace pulsar
