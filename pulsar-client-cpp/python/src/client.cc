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

#include "utils.h"

Producer Client_createProducer(Client& client, const std::string& topic, const ProducerConfiguration& conf) {
    Producer producer;
    CHECK_RESULT(client.createProducer(topic, conf, producer));
    return producer;
}

Consumer Client_subscribe(Client& client, const std::string& topic, const std::string& subscriptionName,
                          const ConsumerConfiguration& conf) {
    Consumer consumer;
    CHECK_RESULT(client.subscribe(topic, subscriptionName, conf, consumer));
    return consumer;
}

void Client_close(Client& client) {
    CHECK_RESULT(client.close());
}

void export_client() {
    using namespace boost::python;

    class_<Client>("Client", init<const std::string&, const ClientConfiguration& >())
            .def("create_producer", &Client_createProducer)
            .def("subscribe", &Client_subscribe)
            .def("close", &Client_close)
            .def("shutdown", &Client::shutdown)
            ;
}
