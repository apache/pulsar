#include <lib/ConsumerStatsBase.h>

namespace pulsar {
class ConsumerStatsImpl : public ConsumerStatsBase {
private:
uint64_t numMsgsReceived;
uint64_t numBytesReceived;
uint64_t numReceiveFailed;
uint64_t numAcksSent;
uint64_t numAcksFailed;
uint64_t totalMsgsReceived;
uint64_t totalBytesReceived;
uint64_t totalReceiveFailed;
uint64_t totalAcksSent;
uint64_t totalAcksFailed;
};


}
