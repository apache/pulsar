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

#include "ProducerImpl.h"
#include "ClientImpl.h"
#include "DestinationName.h"
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <pulsar/MessageRoutingPolicy.h>

namespace pulsar {

  class PartitionedProducerImpl: public ProducerImplBase, public boost::enable_shared_from_this<PartitionedProducerImpl> {

 public:
    enum PartitionedProducerState {
      Pending,
      Ready,
      Closing,
      Closed,
      Failed
    };
    const static std::string PARTITION_NAME_SUFFIX;

    typedef boost::unique_lock<boost::mutex> Lock;

    PartitionedProducerImpl(ClientImplPtr ptr,
                            const DestinationNamePtr destinationName,
                            const unsigned int numPartitions,
                            const ProducerConfiguration& config);
    virtual ~PartitionedProducerImpl();

    virtual void sendAsync(const Message& msg, SendCallback callback);

    /*
     * closes all active producers, it can be called explicitly from client as well as createProducer
     * when it fails to create one of the producers and we want to fail createProducer
     */
    virtual void closeAsync(CloseCallback closeCallback);

    virtual void start();

    virtual void shutdown();

    virtual bool isClosed();

    virtual const std::string& getTopic() const;

    virtual Future<Result, ProducerImplBaseWeakPtr> getProducerCreatedFuture();

    void handleSinglePartitionProducerCreated(Result result,
                                              ProducerImplBaseWeakPtr producerBaseWeakPtr,
                                              const unsigned int partitionIndex);

    void handleSinglePartitionProducerClose(Result result,
                                            const unsigned int partitionIndex,
                                            CloseCallback callback);

    void notifyResult(CloseCallback closeCallback);

    void setState(PartitionedProducerState state);

    friend class PulsarFriend;

 private:
    const ClientImplPtr client_;

    const DestinationNamePtr destinationName_;
    const std::string topic_;

    const unsigned int numPartitions_;

    unsigned int numProducersCreated_;

    /*
     * set when one or more Single Partition Creation fails, close will cleanup and fail the create callbackxo
     */
    bool cleanup_;

    const ProducerConfiguration conf_;

    typedef std::vector<ProducerImplPtr> ProducerList;

    ProducerList producers_;

    MessageRoutingPolicyPtr routerPolicy_;

    // mutex_ is used to share state_, and numProducersCreated_
    boost::mutex mutex_;

    PartitionedProducerState state_;

    // only set this promise to value, when producers on all partitions are created.
    Promise<Result, ProducerImplBaseWeakPtr> partitionedProducerCreatedPromise_;
  };

} //ends namespace Pulsar
