//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

/*
#cgo CFLAGS: -I../../pulsar-client-cpp/include
#cgo LDFLAGS: -lpulsar -L../../pulsar-client-cpp/lib
#include "c_go_pulsar.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
)

/// Private

func newClient() ClientBuilder {
	builder := clientBuilder{
		ptr: C.pulsar_client_configuration_create(),
	}

	runtime.SetFinalizer(&builder, builderFinalizer)
	return &builder
}

type clientBuilder struct {
	serviceUrl string
	ptr        *C.pulsar_client_configuration_t
}

func builderFinalizer(cb *clientBuilder) {
	C.pulsar_client_configuration_free(cb.ptr)
}

func (cb *clientBuilder) Build() Client {
	client := &client{
		ptr: C.pulsar_client_create(C.CString(cb.serviceUrl), cb.ptr),
	}

	runtime.SetFinalizer(client, clientFinalizer)
	return client
}

func (cb *clientBuilder) ServiceUrl(serviceUrl string) ClientBuilder {
	cb.serviceUrl = serviceUrl
	return cb
}

func (cb *clientBuilder) IoThreads(ioThreads int) ClientBuilder {
	C.pulsar_client_configuration_set_io_threads(cb.ptr, C.int(ioThreads))
	return cb
}

func (cb *clientBuilder) OperationTimeoutSeconds(operationTimeoutSeconds int) ClientBuilder {
	C.pulsar_client_configuration_set_operation_timeout_seconds(cb.ptr, C.int(operationTimeoutSeconds))
	return cb
}

func (cb *clientBuilder) MessageListenerThreads(messageListenerThreads int) ClientBuilder {
	C.pulsar_client_configuration_set_message_listener_threads(cb.ptr, C.int(messageListenerThreads))
	return cb
}

func (cb *clientBuilder) ConcurrentLookupRequests(concurrentLookupRequests int) ClientBuilder {
	C.pulsar_client_configuration_set_concurrent_lookup_request(cb.ptr, C.int(concurrentLookupRequests))
	return cb
}

func (cb *clientBuilder) LogConfFilePath(logConfFilePath string) ClientBuilder {
	cPath := C.CString(logConfFilePath)
	defer C.free(unsafe.Pointer(cPath))
	C.pulsar_client_configuration_set_log_conf_file_path(cb.ptr, cPath)
	return cb
}

func (cb *clientBuilder) EnableTls(enableTls bool) ClientBuilder {
	C.pulsar_client_configuration_set_use_tls(cb.ptr, cBool(enableTls))
	return cb
}

func (cb *clientBuilder) TlsTrustCertsFilePath(tlsTrustCertsFilePath string) ClientBuilder {
	str := C.CString(tlsTrustCertsFilePath)
	defer C.free(unsafe.Pointer(str))
	C.pulsar_client_configuration_set_tls_trust_certs_file_path(cb.ptr, str)
	return cb
}

func (cb *clientBuilder) TlsAllowInsecureConnection(tlsAllowInsecureConnection bool) ClientBuilder {
	C.pulsar_client_configuration_set_tls_allow_insecure_connection(cb.ptr, cBool(tlsAllowInsecureConnection))
	return cb
}

func (cb *clientBuilder) StatsIntervalInSeconds(statsIntervalInSeconds int) ClientBuilder {
	C.pulsar_client_configuration_set_stats_interval_in_seconds(cb.ptr, C.uint(statsIntervalInSeconds))
	return cb
}

/////////////////// Client Implementation

type client struct {
	ptr *C.pulsar_client_t
}

func clientFinalizer(client *client) {
	C.pulsar_client_free(client.ptr)
}

func (client *client) NewProducer() ProducerBuilder {
	return newProducerBuilder(client)
}

func (client *client) NewConsumer() ConsumerBuilder {
	return newConsumerBuilder(client)
}

func (client *client) NewReader() ReaderBuilder {
	return newReaderBuilder(client)
}

func (client *client) Close() error {
	res := C.pulsar_client_close(client.ptr)
	if res != C.pulsar_result_Ok {
		return NewError(res, "Failed to close Pulsar client")
	} else {
		return nil
	}
}
