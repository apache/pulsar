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
package org.apache.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * This class encapsulate some utility functions for
 * <code>ServiceUnit</code> related metadata operations.
 */
public final class ServiceUnitUtils {
    /**
     * <code>ZooKeeper</code> root path for namespace ownership info.
     */
    private static final String OWNER_INFO_ROOT = "/namespace";

    static String path(NamespaceBundle suname) {
        // The ephemeral node path for new namespaces should always have bundle name appended
        return OWNER_INFO_ROOT + "/" + suname.toString();
    }

    public static NamespaceBundle suBundleFromPath(String path, NamespaceBundleFactory factory) {
        String[] parts = path.split("/");
        checkArgument(parts.length > 2);
        checkArgument(parts[1].equals("namespace"));
        checkArgument(parts.length > 4);

        if (parts.length > 5) {
            // this is a V1 path prop/cluster/namespace/hash
            Range<Long> range = getHashRange(parts[5]);
            return factory.getBundle(NamespaceName.get(parts[2], parts[3], parts[4]), range);
        } else {
            // this is a V2 path prop/namespace/hash
            Range<Long> range = getHashRange(parts[4]);
            return factory.getBundle(NamespaceName.get(parts[2], parts[3]), range);
        }
    }

    private static Range<Long> getHashRange(String rangePathPart) {
        String[] endPoints = rangePathPart.split("_");
        checkArgument(endPoints.length == 2, "Malformed bundle hash range path part:" + rangePathPart);
        Long startLong = Long.decode(endPoints[0]);
        Long endLong = Long.decode(endPoints[1]);
        BoundType endType = (endPoints[1].equals(LAST_BOUNDARY)) ? BoundType.CLOSED : BoundType.OPEN;
        return Range.range(startLong, BoundType.CLOSED, endLong, endType);
    }
}
