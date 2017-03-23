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

#include <lib/ClientConnection.h>
#include <vector>

#ifndef PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H
#define PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H

namespace pulsar {
    /*
     * @note - this class is not thread safe.
     */
    class ClientConnectionContainer {
    private:
        size_t connectionsPerBroker_;
        size_t currentIndex_;
        std::vector<ClientConnectionWeakPtr> list_;
    public:
        ClientConnectionContainer(size_t connectionsPerBroker);
        bool isFull();
        ClientConnectionWeakPtr getNext();
        void add(ClientConnectionWeakPtr cnx);
        void remove();
    };
    typedef boost::shared_ptr<ClientConnectionContainer> ClientConnectionContainerPtr;
}

#endif //PULSAR_CPP_CLIENTCONNECTIONCONTAINER_H
