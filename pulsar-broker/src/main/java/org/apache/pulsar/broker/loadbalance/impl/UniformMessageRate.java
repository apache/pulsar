package org.apache.pulsar.broker.loadbalance.impl;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UniformMessageRate implements ModularLoadManagerStrategy {
    @Override
    public Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf) {
        Map<String, BrokerData> brokersData = loadData.getBrokerData();
        MutableObject<String> underloadedBroker = new MutableObject<>();
        MutableDouble minMsgRate = new MutableDouble(Integer.MAX_VALUE);
        MutableDouble minThroughputRate = new MutableDouble(Integer.MAX_VALUE);
        brokersData.forEach((broker, data) -> {
            TimeAverageBrokerData timeAverageData = data.getTimeAverageData();
            double msgRate = timeAverageData.getLongTermMsgRateIn()
                    + timeAverageData.getLongTermMsgRateOut();
            double throughputRate = timeAverageData.getLongTermMsgThroughputIn()
                    + timeAverageData.getLongTermMsgThroughputOut();

            if (((conf.getLoadBalancerMsgRateDifferenceShedderThreshold() > 0) && (msgRate < minMsgRate.getValue()))
                    || ((conf.getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold() > 0)
                    && (throughputRate < minThroughputRate.getValue()))) {
                underloadedBroker.setValue(broker);
                minMsgRate.setValue(msgRate);
                minThroughputRate.setValue(throughputRate);
            }
        });

        return Optional.of(underloadedBroker.getValue());
    }
}
