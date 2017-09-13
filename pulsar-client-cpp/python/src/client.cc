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
#include "utils.h"

Producer Client_createProducer(Client& client, const std::string& topic, const ProducerConfiguration& conf) {
    Producer producer;
    Result res;

    Py_BEGIN_ALLOW_THREADS
    res = client.createProducer(topic, conf, producer);
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
    return producer;
}

Consumer Client_subscribe(Client& client, const std::string& topic, const std::string& subscriptionName,
                          const ConsumerConfiguration& conf) {
    Consumer consumer;
    Result res;

    Py_BEGIN_ALLOW_THREADS
    res = client.subscribe(topic, subscriptionName, conf, consumer);
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
    return consumer;
}

Reader Client_createReader(Client& client, const std::string& topic,
                           const BatchMessageId& startMessageId,
                           const ReaderConfiguration& conf) {
    Reader reader;
    Result res;

    Py_BEGIN_ALLOW_THREADS
    res = client.createReader(topic, startMessageId, conf, reader);
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
    return reader;
}

void Client_close(Client& client) {
    Result res;

    Py_BEGIN_ALLOW_THREADS
    res = client.close();
    Py_END_ALLOW_THREADS

    CHECK_RESULT(res);
}

void export_client() {
    using namespace boost::python;

    class_<Client>("Client", init<const std::string&, const ClientConfiguration& >())
            .def("create_producer", &Client_createProducer)
            .def("subscribe", &Client_subscribe)
            .def("create_reader", &Client_createReader)
            .def("close", &Client_close)
            .def("shutdown", &Client::shutdown)
            ;
}
