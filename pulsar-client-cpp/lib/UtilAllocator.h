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
#ifndef LIB_UTILALLOCATOR_H_
#define LIB_UTILALLOCATOR_H_

#include <boost/aligned_storage.hpp>

class HandlerAllocator : private boost::noncopyable {
   public:
    HandlerAllocator() : inUse_(false) {}

    void* allocate(std::size_t size) {
        if (!inUse_ && size < storage_.size) {
            inUse_ = true;
            return storage_.address();
        } else {
            return ::operator new(size);
        }
    }

    void deallocate(void* pointer) {
        if (pointer == storage_.address()) {
            inUse_ = false;
        } else {
            ::operator delete(pointer);
        }
    }

   private:
    // Storage space used for handler-based custom memory allocation.
    boost::aligned_storage<1024> storage_;
    bool inUse_;
};

template <typename Handler>
class AllocHandler {
   public:
    AllocHandler(HandlerAllocator& a, Handler h) : allocator_(a), handler_(h) {}

    template <typename Arg1>
    void operator()(Arg1 arg1) {
        handler_(arg1);
    }

    template <typename Arg1, typename Arg2>
    void operator()(Arg1 arg1, Arg2 arg2) {
        handler_(arg1, arg2);
    }

    friend void* asio_handler_allocate(std::size_t size, AllocHandler<Handler>* thisHandler) {
        return thisHandler->allocator_.allocate(size);
    }

    friend void asio_handler_deallocate(void* ptr, std::size_t, AllocHandler<Handler>* thisHandler) {
        thisHandler->allocator_.deallocate(ptr);
    }

   private:
    HandlerAllocator& allocator_;
    Handler handler_;
};

#endif /* LIB_UTILALLOCATOR_H_ */
