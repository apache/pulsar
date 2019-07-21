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

#include <pulsar/c/message_id.h>
#include "c_structs.h"

#include <mutex>
#include <sstream>

std::once_flag initialized;

static pulsar_message_id_t earliest;
static pulsar_message_id_t latest;

static void initialize() {
    earliest.messageId = pulsar::MessageId::earliest();
    latest.messageId = pulsar::MessageId::latest();
}

const pulsar_message_id_t *pulsar_message_id_earliest() {
    std::call_once(initialized, &initialize);
    return &earliest;
}

const pulsar_message_id_t *pulsar_message_id_latest() {
    std::call_once(initialized, &initialize);
    return &latest;
}

void *pulsar_message_id_serialize(pulsar_message_id_t *messageId, int *len) {
    std::string str;
    messageId->messageId.serialize(str);
    void *p = malloc(str.length());
    memcpy(p, str.c_str(), str.length());
    *len = str.length();
    return p;
}

pulsar_message_id_t *pulsar_message_id_deserialize(const void *buffer, uint32_t len) {
    std::string strId((const char *)buffer, len);
    pulsar_message_id_t *messageId = new pulsar_message_id_t;
    messageId->messageId = pulsar::MessageId::deserialize(strId);
    return messageId;
}

char *pulsar_message_id_str(pulsar_message_id_t *messageId) {
    std::stringstream ss;
    ss << messageId->messageId;
    std::string s = ss.str();

#ifdef _MSC_VER
    // strndup is not available in MSVC
    char *sdup = (char *)malloc(s.length() + 1);
    memcpy(sdup, s.c_str(), s.length());
    sdup[s.length()] = '\0';
    return sdup;
#else
    return strndup(s.c_str(), s.length());
#endif
}

void pulsar_message_id_free(pulsar_message_id_t *messageId) { delete messageId; }
