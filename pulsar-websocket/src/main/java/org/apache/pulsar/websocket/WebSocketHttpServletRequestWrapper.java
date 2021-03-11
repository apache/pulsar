package org.apache.pulsar.websocket;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;


/**
 * WebSocket HttpServletRequest wrapper.
 */
public class WebSocketHttpServletRequestWrapper extends HttpServletRequestWrapper {

    final static String HTTP_HEADER_NAME = "Authorization";
    final static String TOKEN = "token";

    public WebSocketHttpServletRequestWrapper(HttpServletRequest request) {
        super(request);
    }

    @Override
    public String getHeader(String name) {
        // The Javascript WebSocket client couldn't add the auth param to the request header,
        // so add the auth param in the queryParameter, use the query param `token` value instead of
        // the Authorization header if the Authorization header not exist.
        if (name.equals(HTTP_HEADER_NAME)
                && !((UpgradeHttpServletRequest) this.getRequest()).getHeaders().containsKey(HTTP_HEADER_NAME)) {
            return getRequest().getParameter(TOKEN);
        }
        return super.getHeader(name);
    }
}
