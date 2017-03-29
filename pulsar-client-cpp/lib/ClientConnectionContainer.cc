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

#include <lib/ClientConnectionContainer.h>
#include <lib/LogUtils.h>

DECLARE_LOG_OBJECT()

namespace pulsar {

ClientConnectionContainer::ClientConnectionContainer(size_t connectionsPerBroker)
        : connectionsPerBroker_(connectionsPerBroker),
          currentIndex_(0) {
}

bool ClientConnectionContainer::isFull() {
    return list_.size() >= connectionsPerBroker_;
}

ClientConnectionWeakPtr ClientConnectionContainer::getNext() {
    if (list_.empty()) {
        return ClientConnectionWeakPtr();
    }
    currentIndex_ = (currentIndex_ + 1) % list_.size();
    return list_[currentIndex_];
}

void ClientConnectionContainer::add(ClientConnectionWeakPtr cnx) {
    list_.push_back(cnx);
}

void ClientConnectionContainer::remove() {
    if (list_.empty()) {
        return;
    }
    list_.erase(list_.begin() + currentIndex_);
    currentIndex_ = currentIndex_ % list_.size();
}
}
