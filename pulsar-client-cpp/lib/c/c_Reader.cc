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

#include <pulsar/c/reader.h>
#include <pulsar/Reader.h>

#include "c_structs.h"

const char *pulsar_reader_get_topic(pulsar_reader_t *reader) { return reader->reader.getTopic().c_str(); }

pulsar_result pulsar_reader_read_next(pulsar_reader_t *reader, pulsar_message_t **msg) {
    pulsar::Message message;
    pulsar::Result res = reader->reader.readNext(message);
    if (res == pulsar::ResultOk) {
        (*msg) = new pulsar_message_t;
        (*msg)->message = message;
    }
    return (pulsar_result)res;
}

pulsar_result pulsar_reader_read_next_with_timeout(pulsar_reader_t *reader, pulsar_message_t **msg,
                                                   int timeoutMs) {
    pulsar::Message message;
    pulsar::Result res = reader->reader.readNext(message, timeoutMs);
    if (res == pulsar::ResultOk) {
        (*msg) = new pulsar_message_t;
        (*msg)->message = message;
    }
    return (pulsar_result)res;
}

pulsar_result pulsar_reader_close(pulsar_reader_t *reader) { return (pulsar_result)reader->reader.close(); }

void pulsar_reader_close_async(pulsar_reader_t *reader, pulsar_result_callback callback, void *ctx) {
    reader->reader.closeAsync(std::bind(handle_result_callback, std::placeholders::_1, callback, ctx));
}

void pulsar_reader_free(pulsar_reader_t *reader) { delete reader; }

pulsar_result pulsar_reader_has_message_available(pulsar_reader_t *reader, int *available) {
    bool isAvailable;
    pulsar_result result = (pulsar_result)reader->reader.hasMessageAvailable(isAvailable);
    *available = isAvailable;
    return result;
}
