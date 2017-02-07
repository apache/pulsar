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

#include "MessageImpl.h"

namespace pulsar {

    MessageImpl::MessageImpl()
        : metadata(),
          payload(),
          messageId(),
          cnx_(0) {
    }

    const Message::StringMap& MessageImpl::properties() {
        if (properties_.size() == 0) {
            for (int i = 0; i < metadata.properties_size(); i++) {
                const std::string& key = metadata.properties(i).key();
                const std::string& value = metadata.properties(i).value();
                properties_.insert(std::make_pair(key, value));
            }
        }
        return properties_;
    }

    const std::string& MessageImpl::getPartitionKey() const {
            return metadata.partition_key();
    }

    bool MessageImpl::hasPartitionKey() const {
        return metadata.has_partition_key();
    }

    uint64_t MessageImpl::getPublishTimestamp() const {
        if (metadata.has_publish_time()) {
            return metadata.publish_time();
        } else {
            return 0ull;
        }
    }
}
