package org.apache.pulsar.functions.api.streamlet.windowing;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.SerDe;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public class WindowContextImpl implements WindowContext{

    private Context context;

    public WindowContextImpl(Context context) {
        this.context = context;
    }
    @Override
    public String getFunctionName() {
        return this.context.getFunctionName();
    }

    @Override
    public String getFunctionId() {
        return this.context.getFunctionId();
    }

    @Override
    public String getInstanceId() {
        return this.context.getInstanceId();
    }

    @Override
    public String getFunctionVersion() {
        return this.getFunctionVersion();
    }

    @Override
    public long getMemoryLimit() {
        return this.getMemoryLimit();
    }

    @Override
    public long getTimeBudgetInMs() {
        return this.getTimeBudgetInMs();
    }

    @Override
    public long getRemainingTimeInMs() {
        return this.getRemainingTimeInMs();
    }

    @Override
    public Logger getLogger() {
        return this.getLogger();
    }

    @Override
    public String getUserConfigValue(String key) {
        return this.getUserConfigValue(key);
    }

    @Override
    public void recordMetric(String metricName, double value) {
        this.context.recordMetric(metricName, value);
    }

    @Override
    public CompletableFuture<Void> publish(String topicName, Object object, Class<? extends SerDe> serDeClass) {
        return this.context.publish(topicName, object, serDeClass);
    }
}
