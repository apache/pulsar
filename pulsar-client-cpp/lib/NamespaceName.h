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
#ifndef _PULSAR_NAMESPACE_NAME_HEADER_
#define _PULSAR_NAMESPACE_NAME_HEADER_

#include <pulsar/defines.h>
#include "ServiceUnitId.h"

#include <memory>
#include <string>

namespace pulsar {

class PULSAR_PUBLIC NamespaceName : public ServiceUnitId {
   public:
    std::shared_ptr<NamespaceName> getNamespaceObject();
    std::string getProperty();
    std::string getCluster();
    std::string getLocalName();
    static std::shared_ptr<NamespaceName> get(const std::string& property, const std::string& cluster,
                                              const std::string& namespaceName);
    static std::shared_ptr<NamespaceName> get(const std::string& property, const std::string& namespaceName);
    bool operator==(const NamespaceName& namespaceName);
    bool isV2();
    std::string toString();

   private:
    std::string namespace_;
    std::string property_;
    std::string cluster_;
    std::string localName_;
    static bool validateNamespace(const std::string& property, const std::string& cluster,
                                  const std::string& namespace_);
    static bool validateNamespace(const std::string& property, const std::string& namespace_);
    NamespaceName(const std::string& property, const std::string& cluster, const std::string& namespace_);
    NamespaceName(const std::string& property, const std::string& namespace_);
};

typedef std::shared_ptr<NamespaceName> NamespaceNamePtr;

}  // namespace pulsar

#endif
