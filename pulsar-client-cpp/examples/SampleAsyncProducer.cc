/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <boost/bind.hpp>

#include <pulsar/Client.h>
#include <pulsar/MessageBuilder.h>

#include <lib/LogUtils.h>

DECLARE_LOG_OBJECT()

using namespace pulsar;

void callback(Result code, const Message& msg) {
    LOG_INFO("Received code: " << code << " -- Msg: " << msg);
}

int main() {
    Client client("pulsar://localhost:6650");

    Producer producer;
    Result result = client.createProducer("persistent://prop/r1/ns1/my-topic", producer);
    if (result != ResultOk) {
        LOG_ERROR("Error creating producer: " << result);
        return -1;
    }

    // Send asynchronously
    while (true) {
        Message msg = MessageBuilder().setContent("content").setProperty("x", "1").build();
        producer.sendAsync(msg, callback);

        sleep(1);
    }
}
