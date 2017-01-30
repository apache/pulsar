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

#ifndef LIB_UNBOUNDEDBLOCKINGQUEUE_H_
#define LIB_UNBOUNDEDBLOCKINGQUEUE_H_

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/circular_buffer.hpp>
// For struct QueueNotEmpty
#include "BlockingQueue.h"

template<typename T>
class UnboundedBlockingQueue {
 public:
    typedef typename boost::circular_buffer<T> Container;
    typedef typename Container::iterator iterator;
    typedef typename Container::const_iterator const_iterator;

    UnboundedBlockingQueue(size_t maxSize)
            : mutex_(),
              queue_(maxSize) {
    }

    ~UnboundedBlockingQueue() {
        Lock lock(mutex_);
        queue_.clear();
    }

    void push(const T& value) {
        Lock lock(mutex_);
        // If the queue is full, wait for space to be available
        bool wasEmpty = queue_.empty();
        if (queue_.full()) {
            queue_.set_capacity(queue_.size() * 2);
        }
        queue_.push_back(value);
        lock.unlock();

        if (wasEmpty) {
            // Notify that an element is pushed
            queueEmptyCondition_.notify_one();
        }
    }

    void pop() {
        Lock lock(mutex_);
        // If the queue is empty, wait until an element is available to be popped
        queueEmptyCondition_.wait(lock, QueueNotEmpty<UnboundedBlockingQueue<T> >(*this));
        queue_.pop_front();
        lock.unlock();
    }

    void pop(T& value) {
        Lock lock(mutex_);
        // If the queue is empty, wait until an element is available to be popped
        queueEmptyCondition_.wait(lock, QueueNotEmpty<UnboundedBlockingQueue<T> >(*this));
        value = queue_.front();
        queue_.pop_front();
        lock.unlock();
    }

    bool pop(T& value, const boost::posix_time::time_duration& timeout) {
        Lock lock(mutex_);
        if (!queueEmptyCondition_.timed_wait(lock, timeout,
                                            QueueNotEmpty<UnboundedBlockingQueue<T> >(*this))) {
            return false;
        }

        value = queue_.front();
        queue_.pop_front();
        lock.unlock();

        return true;
    }

    // Check the 1st element of the queue
    bool peek(T& value) {
        Lock lock(mutex_);
        if (queue_.empty()) {
            return false;
        }

        value = queue_.front();
        return true;
    }

    // Remove all elements from the queue
    void clear() {
        Lock lock(mutex_);
        queue_.clear();
    }

    size_t size() const {
        Lock lock(mutex_);
        return queue_.size();
    }

    bool empty() const {
        Lock lock(mutex_);
        return isEmptyNoMutex();
    }

    const_iterator begin() const {
        return queue_.begin();
    }

    const_iterator end() const {
        return queue_.end();
    }

    iterator begin() {
        return queue_.begin();
    }

    iterator end() {
        return queue_.end();
    }

 private:

    bool isEmptyNoMutex() const {
        return queue_.empty();
    }

    mutable boost::mutex mutex_;
    boost::condition_variable queueEmptyCondition_;
    Container queue_;

    typedef boost::unique_lock<boost::mutex> Lock;
    friend struct QueueNotEmpty<UnboundedBlockingQueue<T> > ;
};

#endif /* LIB_BLOCKINGQUEUE_H_ */
