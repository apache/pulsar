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

#include "TimeUtils.h"

namespace pulsar {

ptime TimeUtils::now() { return microsec_clock::universal_time(); }

int64_t TimeUtils::currentTimeMillis() {
    static ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));

    time_duration diff = now() - time_t_epoch;
    return diff.total_milliseconds();
}
}  // namespace pulsar