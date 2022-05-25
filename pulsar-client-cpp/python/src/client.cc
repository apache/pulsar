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

    waitForAsyncValue(std::function<void(CreateProducerCallback)>([&](CreateProducerCallback callback) {
                          client.createProducerAsync(topic, conf, callback);
                      }),
                      producer);

    return producer;
}

Consumer Client_subscribe(Client& client, const std::string& topic, const std::string& subscriptionName,
                          const ConsumerConfiguration& conf) {
    Consumer consumer;

    waitForAsyncValue(std::function<void(SubscribeCallback)>([&](SubscribeCallback callback) {
                          client.subscribeAsync(topic, subscriptionName, conf, callback);
                      }),
                      consumer);

    return consumer;
}

Consumer Client_subscribe_topics(Client& client, boost::python::list& topics,
                                 const std::string& subscriptionName, const ConsumerConfiguration& conf) {
    std::vector<std::string> topics_vector;
    for (int i = 0; i < len(topics); i++) {
        std::string content = boost::python::extract<std::string>(topics[i]);
        topics_vector.push_back(content);
    }

    Consumer consumer;

    waitForAsyncValue(std::function<void(SubscribeCallback)>([&](SubscribeCallback callback) {
                          client.subscribeAsync(topics_vector, subscriptionName, conf, callback);
                      }),
                      consumer);

    return consumer;
}

Consumer Client_subscribe_pattern(Client& client, const std::string& topic_pattern,
                                  const std::string& subscriptionName, const ConsumerConfiguration& conf) {
    Consumer consumer;

    waitForAsyncValue(std::function<void(SubscribeCallback)>([&](SubscribeCallback callback) {
                          client.subscribeWithRegexAsync(topic_pattern, subscriptionName, conf, callback);
                      }),
                      consumer);

    return consumer;
}

Reader Client_createReader(Client& client, const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf) {
    Reader reader;

    waitForAsyncValue(std::function<void(ReaderCallback)>([&](ReaderCallback callback) {
                          client.createReaderAsync(topic, startMessageId, conf, callback);
                      }),
                      reader);

    return reader;
}

boost::python::list Client_getTopicPartitions(Client& client, const std::string& topic) {
    std::vector<std::string> partitions;

    waitForAsyncValue(std::function<void(GetPartitionsCallback)>([&](GetPartitionsCallback callback) {
                          client.getPartitionsForTopicAsync(topic, callback);
                      }),
                      partitions);

    boost::python::list pyList;
    for (int i = 0; i < partitions.size(); i++) {
        pyList.append(boost::python::object(partitions[i]));
    }

    return pyList;
}

void Client_close(Client& client) {
    waitForAsyncResult([&](ResultCallback callback) { client.closeAsync(callback); });
}

void export_client() {
    using namespace boost::python;

    class_<Client>("Client", init<const std::string&, const ClientConfiguration&>())
        .def("create_producer", &Client_createProducer)
        .def("subscribe", &Client_subscribe)
        .def("subscribe_topics", &Client_subscribe_topics)
        .def("subscribe_pattern", &Client_subscribe_pattern)
        .def("create_reader", &Client_createReader)
        .def("get_topic_partitions", &Client_getTopicPartitions)
        .def("close", &Client_close)
        .def("shutdown", &Client::shutdown);
}
