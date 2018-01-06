package org.apache.pulsar.functions.runtime.worker.request;

public class MarkerRequest extends ServiceRequest{
    private static final long serialVersionUID = -6074909284607487042L;

    public MarkerRequest(String workerId, String requestId) {
        super(workerId, null, ServiceRequestType.MARKER, requestId);
    }

    @Override
    public String toString() {
        return "MarkerRequest{"
                + super.toString()
                + "}";
    }
}
