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
#include "NamespaceName.h"
#include "NamedEntity.h"
#include "LogUtils.h"

#include <boost/algorithm/string.hpp>
#include <memory>
#include <vector>
#include <iostream>
#include <sstream>

DECLARE_LOG_OBJECT()
namespace pulsar {

std::shared_ptr<NamespaceName> NamespaceName::get(const std::string& property, const std::string& cluster,
                                                  const std::string& namespaceName) {
    if (validateNamespace(property, cluster, namespaceName)) {
        std::shared_ptr<NamespaceName> ptr(new NamespaceName(property, cluster, namespaceName));
        return ptr;
    } else {
        LOG_DEBUG("Returning a null NamespaceName object");
        return std::shared_ptr<NamespaceName>();
    }
}

NamespaceName::NamespaceName(const std::string& property, const std::string& cluster,
                             const std::string& namespaceName) {
    std::ostringstream oss;
    oss << property << "/" << cluster << "/" << namespaceName;
    this->namespace_ = oss.str();
    this->property_ = property;
    this->cluster_ = cluster;
    this->localName_ = namespaceName;
}

bool NamespaceName::validateNamespace(const std::string& property, const std::string& cluster,
                                      const std::string& namespaceName) {
    if (!property.empty() && !cluster.empty() && !namespaceName.empty()) {
        return NamedEntity::checkName(property) && NamedEntity::checkName(cluster) &&
               NamedEntity::checkName(namespaceName);
    } else {
        LOG_DEBUG("Empty parameters passed for validating namespace");
        return false;
    }
}

std::shared_ptr<NamespaceName> NamespaceName::get(const std::string& property,
                                                  const std::string& namespaceName) {
    if (validateNamespace(property, namespaceName)) {
        std::shared_ptr<NamespaceName> ptr(new NamespaceName(property, namespaceName));
        return ptr;
    } else {
        LOG_DEBUG("Returning a null NamespaceName object");
        return std::shared_ptr<NamespaceName>();
    }
}

NamespaceName::NamespaceName(const std::string& property, const std::string& namespaceName) {
    std::ostringstream oss;
    oss << property << "/" << namespaceName;
    this->namespace_ = oss.str();
    this->property_ = property;
    this->localName_ = namespaceName;
}

bool NamespaceName::validateNamespace(const std::string& property, const std::string& namespaceName) {
    if (!property.empty() && !namespaceName.empty()) {
        return NamedEntity::checkName(property) && NamedEntity::checkName(namespaceName);
    } else {
        LOG_DEBUG("Empty parameters passed for validating namespace");
        return false;
    }
}

std::shared_ptr<NamespaceName> NamespaceName::getNamespaceObject() {
    return std::shared_ptr<NamespaceName>(this);
}

bool NamespaceName::operator==(const NamespaceName& namespaceName) {
    return this->namespace_.compare(namespaceName.namespace_) == 0;
}

std::string NamespaceName::getProperty() { return this->property_; }

std::string NamespaceName::getCluster() { return this->cluster_; }

std::string NamespaceName::getLocalName() { return this->localName_; }

bool NamespaceName::isV2() { return this->cluster_.empty(); }

std::string NamespaceName::toString() { return this->namespace_; }

}  // namespace pulsar
