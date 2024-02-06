/*
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
package org.apache.pulsar.common.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;

/**
 *  Utility class to match OpenTelemetry metric data based on a set of optional criteria. If a criterion is set, the
 *  input MetricData must match it in order for the predicate to evaluate true.
 */
@Builder
public class MetricDataMatcher implements Predicate<MetricData> {
    private final String name;
    private final MetricDataType type;
    private final InstrumentationScopeInfo instrumentationScopeInfo;
    private final Resource resource;
    @Singular
    private final List<Attributes> resourceAttributes;
    @Singular
    private final List<Attributes> dataAttributes;
    @Singular
    private final List<Long> longValues;
    @Singular
    private final List<Double> doubleValues;

    @Override
    public boolean test(MetricData md) {
        return (name == null || name.equals(md.getName()))
                && (type == null || type.equals(md.getType()))
                && (instrumentationScopeInfo == null
                || instrumentationScopeInfo.equals(md.getInstrumentationScopeInfo()))
                && (resource == null || resource.equals(md.getResource()))
                && matchesResourceAttributes(md)
                && (dataAttributes == null || matchesDataAttributes(md))
                && matchesLongValues(md)
                && matchesDoubleValues(md);
    }

    private boolean matchesResourceAttributes(MetricData md) {
        Attributes actual = md.getResource().getAttributes();
        return resourceAttributes.stream().allMatch(expected -> matchesAttributes(actual, expected));
    }

    private boolean matchesDataAttributes(MetricData md) {
        Collection<Attributes> actuals =
                md.getData().getPoints().stream().map(PointData::getAttributes).collect(Collectors.toSet());
        return dataAttributes.stream().
                allMatch(expected -> actuals.stream().anyMatch(actual -> matchesAttributes(actual, expected)));
    }

    private boolean matchesAttributes(Attributes actual, Attributes expected) {
        // Return true iff all attribute pairs in expected are a subset of those in actual. Allows tests to specify
        // just the attributes they care about, instead of exhaustively having to list all of them.
        if (true) {
            for (var entry : expected.asMap().entrySet()) {
                var key = entry.getKey();
                var value = entry.getValue();
                var actualValue = actual.get(key);
                if (!value.equals(actualValue)) {
                    return false;
                }
            }
            return true;
        }
        return expected.asMap().entrySet().stream().allMatch(e -> e.getValue().equals(actual.get(e.getKey())));
    }

    private boolean matchesLongValues(MetricData md) {
        Collection<Long> actualData =
                md.getLongSumData().getPoints().stream().map(LongPointData::getValue).collect(Collectors.toSet());
        return actualData.containsAll(longValues);
    }

    private boolean matchesDoubleValues(MetricData md) {
        Collection<Double> actualData =
                md.getDoubleSumData().getPoints().stream().map(DoublePointData::getValue).collect(Collectors.toSet());
        return actualData.containsAll(doubleValues);
    }
}
