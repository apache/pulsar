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

#ifndef MESSAGE_BUILDER_H
#define MESSAGE_BUILDER_H

#include <vector>
#include "Message.h"

#pragma GCC visibility push(default)

namespace pulsar {

class MessageBuilder {
 public:

    MessageBuilder();

    typedef std::map<std::string, std::string> StringMap;

    /**
     * Finalize the immutable message
     */
    Message build();

    /**
     * Set content of the message. The message contents will be managed by the system.
     */
    MessageBuilder& setContent(const void* data, size_t size);
    MessageBuilder& setContent(const std::string& data);

    /**
     * Set content of the message to a buffer already allocated by the caller. No copies of
     * this buffer will be made. The caller is responsible to ensure the memory buffer is
     * valid until the message has been persisted (or an error is returned).
     */
    MessageBuilder& setAllocatedContent(void* data, size_t size);

    /**
     * Sets a new property on a message.
     * @param name   the name of the property
     * @param value  the associated value
     */
    MessageBuilder& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    MessageBuilder& setProperties(const StringMap& properties);

    /*
     * set partition key for the message routing
     * @param hash of this key is used to determine message's destination partition
     */
    MessageBuilder& setPartitionKey(const std::string& partitionKey);

    /**
     * override namespace replication clusters.  note that it is the
     * caller's responsibility to provide valid cluster names, and that
     * all clusters have been previously configured as destinations.
     *
     * given an empty list, the message will replicate per the namespace
     * configuration.
     *
     * @param clusters where to send this message.
     */
    MessageBuilder& setReplicationClusters(const std::vector<std::string>& clusters);

    /**
     * Do not replicate this message
     * @param flag if true, disable replication, otherwise use default
     * replication
     */
    MessageBuilder& disableReplication(bool flag);

    /**
     * create a empty message, with no properties or data
     *
     */
    MessageBuilder& create();
 private:
    MessageBuilder(const MessageBuilder&);
    void checkMetadata();

    Message::MessageImplPtr impl_;
};

}

#pragma GCC visibility pop

#endif
