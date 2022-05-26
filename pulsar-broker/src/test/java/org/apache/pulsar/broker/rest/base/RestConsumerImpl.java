package org.apache.pulsar.broker.rest.base;

import org.apache.pulsar.broker.rest.base.api.RestConsumer;
import org.apache.pulsar.client.admin.internal.BaseResource;
import org.apache.pulsar.client.api.Authentication;
import javax.ws.rs.client.WebTarget;

public class RestConsumerImpl extends BaseResource implements RestConsumer {

    private final WebTarget admin;

    public RestConsumerImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.admin = web.path("/topics");
    }
}
