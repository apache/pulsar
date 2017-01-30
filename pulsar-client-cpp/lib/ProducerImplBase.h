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

#ifndef PULSAR_PRODUCER_IMPL_BASE_HEADER
#define PULSAR_PRODUCER_IMPL_BASE_HEADER
#include <pulsar/Message.h>
#include <pulsar/Producer.h>

namespace pulsar {
class ProducerImplBase;

typedef boost::weak_ptr<ProducerImplBase> ProducerImplBaseWeakPtr;
typedef boost::shared_ptr<ProducerImplBase> ProducerImplBasePtr;

class ProducerImplBase {
public:
  virtual ~ProducerImplBase(){
  }

  virtual void sendAsync(const Message& msg, SendCallback callback) = 0;
  virtual void closeAsync(CloseCallback callback) = 0;
  virtual void start() = 0;
  virtual void shutdown() = 0;
  virtual bool isClosed() = 0;
  virtual const std::string& getTopic() const = 0;
  virtual Future<Result, ProducerImplBaseWeakPtr> getProducerCreatedFuture() = 0;
};
}
#endif //PULSAR_PRODUCER_IMPL_BASE_HEADER
