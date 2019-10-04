

#include "TimeUtils.h"

namespace pulsar {

ptime TimeUtils::now() { return microsec_clock::universal_time(); }

int64_t TimeUtils::currentTimeMillis() {
    static ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));

    time_duration diff = now() - time_t_epoch;
    return diff.total_milliseconds();
}
}  // namespace pulsar