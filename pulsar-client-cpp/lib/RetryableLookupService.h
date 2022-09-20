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
#pragma once

#include <algorithm>
#include <memory>
#include "lib/Backoff.h"
#include "lib/ExecutorService.h"
#include "lib/LookupService.h"
#include "lib/SynchronizedHashMap.h"
#include "lib/LogUtils.h"

namespace pulsar {

class RetryableLookupService : public LookupService,
                               public std::enable_shared_from_this<RetryableLookupService> {
   private:
    friend class PulsarFriend;
    struct PassKey {
        explicit PassKey() {}
    };

   public:
    template <typename... Args>
    explicit RetryableLookupService(PassKey, Args&&... args)
        : RetryableLookupService(std::forward<Args>(args)...) {}

    template <typename... Args>
    static std::shared_ptr<RetryableLookupService> create(Args&&... args) {
        return std::make_shared<RetryableLookupService>(PassKey{}, std::forward<Args>(args)...);
    }

    LookupResultFuture getBroker(const TopicName& topicName) override {
        return executeAsync<LookupResult>("get-broker-" + topicName.toString(),
                                          [this, topicName] { return lookupService_->getBroker(topicName); });
    }

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) override {
        return executeAsync<LookupDataResultPtr>(
            "get-partition-metadata-" + topicName->toString(),
            [this, topicName] { return lookupService_->getPartitionMetadataAsync(topicName); });
    }

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName) override {
        return executeAsync<NamespaceTopicsPtr>(
            "get-topics-of-namespace-" + nsName->toString(),
            [this, nsName] { return lookupService_->getTopicsOfNamespaceAsync(nsName); });
    }

    template <typename T>
    Future<Result, T> executeAsync(const std::string& key, std::function<Future<Result, T>()> f) {
        Promise<Result, T> promise;
        executeAsyncImpl(key, f, promise, timeout_);
        return promise.getFuture();
    }

   private:
    const std::shared_ptr<LookupService> lookupService_;
    const TimeDuration timeout_;
    Backoff backoff_;
    const ExecutorServiceProviderPtr executorProvider_;

    using Timer = boost::asio::deadline_timer;
    using TimerPtr = std::unique_ptr<Timer>;
    SynchronizedHashMap<std::string, TimerPtr> backoffTimers_;

    RetryableLookupService(std::shared_ptr<LookupService> lookupService, int timeoutSeconds,
                           ExecutorServiceProviderPtr executorProvider)
        : lookupService_(lookupService),
          timeout_(boost::posix_time::seconds(timeoutSeconds)),
          backoff_(boost::posix_time::milliseconds(100), timeout_ + timeout_,
                   boost::posix_time::milliseconds(0)),
          executorProvider_(executorProvider) {}

    std::weak_ptr<RetryableLookupService> weak_from_this() noexcept { return shared_from_this(); }

    // NOTE: Set the visibility to fix compilation error in GCC 6
    template <typename T>
#ifndef _WIN32
    __attribute__((visibility("hidden")))
#endif
    void
    executeAsyncImpl(const std::string& key, std::function<Future<Result, T>()> f, Promise<Result, T> promise,
                     TimeDuration remainingTime) {
        auto weakSelf = weak_from_this();
        f().addListener([this, weakSelf, key, f, promise, remainingTime](Result result, const T& value) {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }

            if (result == ResultOk) {
                backoffTimers_.remove(key);
                promise.setValue(value);
            } else if (result == ResultRetryable) {
                if (remainingTime.total_milliseconds() <= 0) {
                    backoffTimers_.remove(key);
                    promise.setFailed(ResultTimeout);
                    return;
                }

                auto it = backoffTimers_.emplace(
                    key, TimerPtr{new Timer(executorProvider_->get()->getIOService())});
                auto& timer = *(it.first->second);
                auto delay = std::min(backoff_.next(), remainingTime);
                timer.expires_from_now(delay);

                auto nextRemainingTime = remainingTime - delay;
                LOG_INFO("Reschedule " << key << " for " << delay.total_milliseconds()
                                       << " ms, remaining time: " << nextRemainingTime.total_milliseconds()
                                       << " ms");
                timer.async_wait([this, weakSelf, key, f, promise,
                                  nextRemainingTime](const boost::system::error_code& ec) {
                    auto self = weakSelf.lock();
                    if (!self || ec) {
                        if (self && ec != boost::asio::error::operation_aborted) {
                            LOG_ERROR("The timer for " << key << " failed: " << ec.message());
                        }
                        // The lookup service has been destructed or the timer has been cancelled
                        promise.setFailed(ResultTimeout);
                        return;
                    }
                    executeAsyncImpl(key, f, promise, nextRemainingTime);
                });
            } else {
                backoffTimers_.remove(key);
                promise.setFailed(result);
            }
        });
    }

    DECLARE_LOG_OBJECT()
};

}  // namespace pulsar
