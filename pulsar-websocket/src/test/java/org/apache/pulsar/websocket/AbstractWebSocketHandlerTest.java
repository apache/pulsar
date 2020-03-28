package org.apache.pulsar.websocket;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author: feynmamnlin
 * @date: 2020/3/28 11:54 上午
 */
public class AbstractWebSocketHandlerTest {
    @Mock
    private HttpServletRequest httpServletRequest;

    @Test
    public void parseTopicNameTest() {
        String producerV1 = "/ws/producer/persistent/my-property/my-cluster/my-ns/my-topic";
        String consumerV1 = "/ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription";
        String readerV1 = "/ws/reader/persistent/my-property/my-cluster/my-ns/my-topic";

        String producerV2 = "/ws/v2/producer/persistent/my-property/my-ns/my-topic";
        String consumerV2 = "/ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription";
        String consumerLongTopicNameV2 = "/ws/v2/consumer/persistent/my-tenant/my-ns/some/topic/with/slashes/my-sub";
        String readerV2 = "/ws/v2/reader/persistent/my-property/my-ns/my-topic";

        httpServletRequest = mock(HttpServletRequest.class);

        when(httpServletRequest.getRequestURI()).thenReturn(producerV1);
        WebSocketHandlerImpl webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        TopicName topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-cluster/my-ns/my-topic", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV1);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-cluster/my-ns/my-topic/my-subscription", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(readerV1);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-cluster/my-ns/my-topic", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(producerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-ns/my-topic", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-ns/my-topic/my-subscription", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(consumerLongTopicNameV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-tenant/my-ns/some/topic/with/slashes/my-sub", topicName.toString());

        when(httpServletRequest.getRequestURI()).thenReturn(readerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        Assert.assertEquals("persistent://my-property/my-ns/my-topic", topicName.toString());

    }

    class WebSocketHandlerImpl extends AbstractWebSocketHandler {

        public WebSocketHandlerImpl(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
            super(service, request, response);
        }

        @Override
        protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        public TopicName getTopic() {
            return super.topic;
        }

    }

}
