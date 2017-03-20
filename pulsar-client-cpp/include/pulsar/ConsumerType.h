//
// Created by Jai Asher on 3/20/17.
//

#ifndef PULSAR_CPP_CONSUMERTYPE_H
#define PULSAR_CPP_CONSUMERTYPE_H

namespace pulsar {
    enum ConsumerType {
        /**
         * There can be only 1 consumer on the same topic with the same consumerName
         */
        ConsumerExclusive,

        /**
         * Multiple consumers will be able to use the same consumerName and the messages
         *  will be dispatched according to a round-robin rotation between the connected consumers
         */
        ConsumerShared,

        /** Only one consumer is active on the subscription; Subscription can have N consumers
         *  connected one of which will get promoted to master if the current master becomes inactive
         */
        ConsumerFailover
    };
}

#endif //PULSAR_CPP_CONSUMERTYPE_H
