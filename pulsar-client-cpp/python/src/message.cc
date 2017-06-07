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

#include <boost/python/suite/indexing/map_indexing_suite.hpp>

std::string MessageId_str(const MessageId& msgId) {
    std::stringstream ss;
    ss << msgId;
    return ss.str();
}

std::string Message_str(const Message& msg) {
    std::stringstream ss;
    ss << msg;
    return ss.str();
}

void export_message() {
    using namespace boost::python;

    MessageBuilder& (MessageBuilder::*MessageBuilderSetContentString)(const std::string&) = &MessageBuilder::setContent;

    class_<MessageBuilder, boost::noncopyable>("MessageBuilder")
            .def("content", MessageBuilderSetContentString, return_self<>())
            .def("property", &MessageBuilder::setProperty, return_self<>())
            .def("properties", &MessageBuilder::setProperties, return_self<>())
            .def("partition_key", &MessageBuilder::setPartitionKey, return_self<>())
            .def("replication_clusters", &MessageBuilder::setReplicationClusters, return_self<>())
            .def("disable_replication", &MessageBuilder::disableReplication, return_self<>())
            .def("build", &MessageBuilder::build)
            ;

    class_<Message::StringMap>("MessageStringMap")
            .def(map_indexing_suite<Message::StringMap>())
            ;

    class_<MessageId>("MessageId")
            .def("__str__", &MessageId_str)
            ;

    class_<Message>("Message")
            .def("properties", &Message::getProperties, return_value_policy<copy_const_reference>())
            .def("data", &Message::getDataAsString)
            .def("length", &Message::getLength)
            .def("partition_key", &Message::getPartitionKey, return_value_policy<copy_const_reference>())
            .def("publish_timestamp", &Message::getPublishTimestamp)
            .def("message_id", &Message::getMessageId, return_value_policy<copy_const_reference>())
            .def("__str__", &Message_str)
            ;
}