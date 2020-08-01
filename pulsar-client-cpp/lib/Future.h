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

#include <functional>
#include <mutex>
#include <memory>
#include <condition_variable>

#include <list>

typedef std::unique_lock<std::mutex> Lock;

namespace pulsar {

template <typename Result, typename Type>
struct InternalState {
    std::mutex mutex;
    std::condition_variable condition;
    Result result;
    Type value;
    bool complete;

    std::list<typename std::function<void(Result, const Type&)> > listeners;
};

template <typename Result, typename Type>
class Future {
   public:
    typedef std::function<void(Result, const Type&)> ListenerCallback;

    Future& addListener(ListenerCallback callback) {
        InternalState<Result, Type>* state = state_.get();
        Lock lock(state->mutex);

        if (state->complete) {
            lock.unlock();
            callback(state->result, state->value);
        } else {
            state->listeners.push_back(callback);
        }

        return *this;
    }

    Result get(Type& result) {
        InternalState<Result, Type>* state = state_.get();
        Lock lock(state->mutex);

        if (!state->complete) {
            // Wait for result
            while (!state->complete) {
                state->condition.wait(lock);
            }
        }

        result = state->value;
        return state->result;
    }

   private:
    typedef std::shared_ptr<InternalState<Result, Type> > InternalStatePtr;
    Future(InternalStatePtr state) : state_(state) {}

    std::shared_ptr<InternalState<Result, Type> > state_;

    template <typename U, typename V>
    friend class Promise;
};

template <typename Result, typename Type>
class Promise {
   public:
    Promise() : state_(std::make_shared<InternalState<Result, Type> >()) {}

    bool setValue(const Type& value) {
        InternalState<Result, Type>* state = state_.get();
        Lock lock(state->mutex);

        if (state->complete) {
            return false;
        }

        state->value = value;
        state->result = Result();
        state->complete = true;

        typename std::list<ListenerCallback>::iterator it;
        for (it = state->listeners.begin(); it != state->listeners.end(); ++it) {
            ListenerCallback& callback = *it;
            callback(state->result, state->value);
        }

        state->listeners.clear();
        state->condition.notify_all();
        return true;
    }

    bool setFailed(Result result) {
        InternalState<Result, Type>* state = state_.get();
        Lock lock(state->mutex);

        if (state->complete) {
            return false;
        }

        state->result = result;
        state->complete = true;

        typename std::list<ListenerCallback>::iterator it;
        for (it = state->listeners.begin(); it != state->listeners.end(); ++it) {
            ListenerCallback& callback = *it;
            callback(state->result, state->value);
        }

        state->listeners.clear();
        state->condition.notify_all();
        return true;
    }

    bool isComplete() const {
        InternalState<Result, Type>* state = state_.get();
        Lock lock(state->mutex);
        return state->complete;
    }

    Future<Result, Type> getFuture() const { return Future<Result, Type>(state_); }

   private:
    typedef std::function<void(Result, const Type&)> ListenerCallback;
    std::shared_ptr<InternalState<Result, Type> > state_;
};

class Void {};

} /* namespace pulsar */

#endif /* LIB_FUTURE_H_ */
