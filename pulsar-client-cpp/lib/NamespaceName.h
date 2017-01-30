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

#ifndef _PULSAR_NAMESPACE_NAME_HEADER_
#define _PULSAR_NAMESPACE_NAME_HEADER_

#include "ServiceUnitId.h"

#include <string>
#include <boost/shared_ptr.hpp>


#pragma GCC visibility push(default)

class NamespaceName : public ServiceUnitId {
 public:
    boost::shared_ptr<NamespaceName> getNamespaceObject();
    std::string getProperty();
    std::string getCluster();
    std::string getLocalName();
    static boost::shared_ptr<NamespaceName> get(const std::string& property,
                                                const std::string& cluster,
                                                const std::string& namespaceName);
    bool operator ==(const NamespaceName& namespaceName);

 private:
    std::string namespace_;
    std::string property_;
    std::string cluster_;
    std::string localName_;
    static bool validateNamespace(const std::string& property, const std::string& cluster,
                                  const std::string& namespace_);
    NamespaceName(const std::string& property, const std::string& cluster,
                  const std::string& namespace_);
};

#pragma GCC visibility pop

#endif
