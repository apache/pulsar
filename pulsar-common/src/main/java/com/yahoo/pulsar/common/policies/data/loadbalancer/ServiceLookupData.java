package com.yahoo.pulsar.common.policies.data.loadbalancer;

// For backwards compatibility purposes.
public interface ServiceLookupData {
	public String getWebServiceUrl();

	public String getWebServiceUrlTls();

	public String getPulsarServiceUrl();

	public String getPulsarServiceUrlTls();
}
