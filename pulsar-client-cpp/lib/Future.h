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
#ifndef LIB_FUTURE_H_
#define LIB_FUTURE_H_

#include <atomic>
#include <functional>
#include <mutex>
#include <memory>
#include <condition_variable>

#include <list>

typedef std::unique_lock<std::mutex> Lock;

namespace pulsar {

template <typename Result, typename Type>
class InternalState {
   public:
    using ListenerCallback = std::function<void(Result, const Type&)>;

    // There's a bug about the defaulted default constructor for GCC < 4.9.1, so we cannot use
    // `InternalState() = default` here.
    InternalState() {}
    InternalState(const InternalState&) = delete;
    InternalState& operator=(const InternalState&) = delete;

    static Result defaultResult() {
        static Result result;
        return result;
    }

    static Type defaultValue() {
        static Type value;
        return value;
    }

    bool completed() const noexcept { return completed_; }

    void addListener(const ListenerCallback& callback) {
        Lock lock(mutex_);
        if (completed_) {
            const auto result = result_;
            const auto value = value_;
            lock.unlock();
            callback(result, value);
        } else {
            listeners_.emplace_back(callback);
        }
    }

    Result wait(Type& value) {
        Lock lock(mutex_);
        while (!completed_) {
            condition_.wait(lock);
        }
        value = value_;
        return result_;
    }

    bool complete(const Type& value) {
        if (completed_) {
            return false;
        }

        Lock lock(mutex_);
        value_ = value;
        completed_ = true;
        auto listeners = std::move(listeners_);
        lock.unlock();

        for (auto& callback : listeners) {
            callback(defaultResult(), value);
        }
        condition_.notify_all();
        return true;
    }

    bool completeExceptionally(Result result) {
        if (completed_) {
            return false;
        }

        Lock lock(mutex_);
        result_ = result;
        completed_ = true;
        auto listeners = std::move(listeners_);
        lock.unlock();

        for (auto& callback : listeners) {
            callback(result, defaultValue());
        }
        condition_.notify_all();
        return true;
    }

   private:
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    Result result_;
    Type value_;
    std::atomic_bool completed_{false};

    std::list<ListenerCallback> listeners_;
};

template <typename Result, typename Type>
class Future {
   public:
    using InternalStateType = InternalState<Result, Type>;
    using InternalStatePtr = std::shared_ptr<InternalStateType>;
    using ListenerCallback = typename InternalStateType::ListenerCallback;

    Future(const Future&) = default;
    Future& operator=(const Future&) = default;

    Future& addListener(const ListenerCallback& callback) {
        state_->addListener(callback);
        return *this;
    }

    Result get(Type& result) { return state_->wait(result); }

   private:
    Future(InternalStatePtr state) : state_(state) {}

    InternalStatePtr state_;

    template <typename U, typename V>
    friend class Promise;
};

template <typename Result, typename Type>
class Promise {
   public:
    using FutureType = Future<Result, Type>;
    using InternalStateType = typename FutureType::InternalStateType;
    using InternalStatePtr = typename FutureType::InternalStatePtr;

    bool setValue(const Type& value) const { return state_->complete(value); }

    bool setFailed(Result result) const { return state_->completeExceptionally(result); }

    bool isComplete() const { return state_->completed(); }

    FutureType getFuture() const { return state_; }

   private:
    InternalStatePtr state_{std::make_shared<InternalStateType>()};
};

class Void {};

} /* namespace pulsar */

#endif /* LIB_FUTURE_H_ */
