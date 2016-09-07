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
package com.yahoo.pulsar.broker.lookup;

import javax.ws.rs.core.Response.Status;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.broker.web.NoSwaggerDocumentation;
import com.yahoo.pulsar.broker.web.RestException;

/**
 * This is the base class for lookup web service
 */
@NoSwaggerDocumentation
public class LookupResource extends PulsarWebResource {

    protected boolean isBundlePolicyActive(NamespaceName fqnn) throws Exception {
        try {
            return pulsar().getLocalZkCacheService().policiesCache()
                    .get(AdminResource.path("policies", fqnn.toString())) != null;
        } catch (NoNodeException nne) {
            // OK if no policies configured
            return false;
        }
    }

    protected void validateNoBundlePolicy(NamespaceName fqnn) throws RestException {
        boolean isBundleActive;
        try {
            isBundleActive = isBundlePolicyActive(fqnn);

        } catch (Exception e) {
            log.error(String.format("Unexpected exception while retrieving bundle policy for namespace %s", fqnn), e);
            throw new RestException(e);
        }

        if (isBundleActive) {
            throw new RestException(Status.PRECONDITION_FAILED, "Lookup on namespace w/ bundle policy is not allowed.");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LookupResource.class);
}
