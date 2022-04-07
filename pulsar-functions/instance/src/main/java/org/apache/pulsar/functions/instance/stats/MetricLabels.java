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

package org.apache.pulsar.functions.instance.stats;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ObjectArrays;

/**
 * Interface for some common label operations.
 */
public interface MetricLabels {

    default String[] combineLabels(String[] labels1, String[] labels2) {
        if (labels1 == null || labels2 == null) {
            return labels1 == null ? labels2 : labels1;
        }
        return ObjectArrays.concat(labels1, labels2, String.class);
    }

    default void checkLabels(String[] labelNames, String[] labels) {
        checkArgument((labelNames == null) == (labels == null),
                "Label names and labels must be both provided or both absent");

        if (labelNames != null) {
            checkArgument(labelNames.length == labels.length,
                    "The number of label names need to match the number of labels");
        }
    }

}
