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
#include <lib/ReaderConfigurationImpl.h>

namespace pulsar {

ReaderConfiguration::ReaderConfiguration() : impl_(std::make_shared<ReaderConfigurationImpl>()) {}

ReaderConfiguration::~ReaderConfiguration() {}

ReaderConfiguration::ReaderConfiguration(const ReaderConfiguration& x) : impl_(x.impl_) {}

ReaderConfiguration& ReaderConfiguration::operator=(const ReaderConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ReaderConfiguration& ReaderConfiguration::setSchema(const SchemaInfo& schemaInfo) {
    impl_->schemaInfo = schemaInfo;
    return *this;
}

const SchemaInfo& ReaderConfiguration::getSchema() const { return impl_->schemaInfo; }

ReaderConfiguration& ReaderConfiguration::setReaderListener(ReaderListener readerListener) {
    impl_->readerListener = readerListener;
    impl_->hasReaderListener = true;
    return *this;
}

ReaderListener ReaderConfiguration::getReaderListener() const { return impl_->readerListener; }

bool ReaderConfiguration::hasReaderListener() const { return impl_->hasReaderListener; }

void ReaderConfiguration::setReceiverQueueSize(int size) { impl_->receiverQueueSize = size; }

int ReaderConfiguration::getReceiverQueueSize() const { return impl_->receiverQueueSize; }

const std::string& ReaderConfiguration::getReaderName() const { return impl_->readerName; }

void ReaderConfiguration::setReaderName(const std::string& readerName) { impl_->readerName = readerName; }

const std::string& ReaderConfiguration::getSubscriptionRolePrefix() const {
    return impl_->subscriptionRolePrefix;
}

void ReaderConfiguration::setSubscriptionRolePrefix(const std::string& subscriptionRolePrefix) {
    impl_->subscriptionRolePrefix = subscriptionRolePrefix;
}

bool ReaderConfiguration::isReadCompacted() const { return impl_->readCompacted; }

void ReaderConfiguration::setReadCompacted(bool compacted) { impl_->readCompacted = compacted; }
}  // namespace pulsar
