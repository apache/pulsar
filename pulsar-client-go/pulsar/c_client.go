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

func newClient(options ClientOptions) (Client, error) {
	if options.URL == "" {
		return nil, newError(C.pulsar_result_InvalidConfiguration, "URL is required for client")
	}

	// Configure the client
	conf := C.pulsar_client_configuration_create()
	if options.IOThreads != 0 {
		C.pulsar_client_configuration_set_io_threads(conf, C.int(options.IOThreads))
	}

	if options.OperationTimeoutSeconds != 0 {
		C.pulsar_client_configuration_set_operation_timeout_seconds(conf, C.int(options.OperationTimeoutSeconds))
	}

	if options.MessageListenerThreads != 0 {
		C.pulsar_client_configuration_set_message_listener_threads(conf, C.int(options.MessageListenerThreads))
	}

	if options.ConcurrentLookupRequests != 0 {
		C.pulsar_client_configuration_set_concurrent_lookup_request(conf, C.int(options.ConcurrentLookupRequests))
	}

	if options.LogConfFilePath != "" {
		cPath := C.CString(options.LogConfFilePath)
		defer C.free(unsafe.Pointer(cPath))
		C.pulsar_client_configuration_set_log_conf_file_path(conf, cPath)
	}

	if options.EnableTLS {
		C.pulsar_client_configuration_set_use_tls(conf, cBool(options.EnableTLS))
	}

	if options.TLSTrustCertsFilePath != "" {
		str := C.CString(options.TLSTrustCertsFilePath)
		defer C.free(unsafe.Pointer(str))
		C.pulsar_client_configuration_set_tls_trust_certs_file_path(conf, str)
	}

	if options.TLSAllowInsecureConnection {
		C.pulsar_client_configuration_set_tls_allow_insecure_connection(conf, cBool(options.TLSAllowInsecureConnection))
	}

	if options.StatsIntervalInSeconds != 0 {
		C.pulsar_client_configuration_set_stats_interval_in_seconds(conf, C.uint(options.StatsIntervalInSeconds))
	}

	client := &client{
		ptr: C.pulsar_client_create(C.CString(options.URL), conf),
	}

	C.pulsar_client_configuration_free(conf)
	runtime.SetFinalizer(client, clientFinalizer)
	return client, nil
}

type client struct {
	ptr *C.pulsar_client_t
}

func clientFinalizer(client *client) {
	C.pulsar_client_free(client.ptr)
}

func (client *client) CreateProducer(options ProducerOptions) (Producer, error) {
	// Create is implemented on async create with a channel to wait for
	// completion without blocking the real thread
	c := make(chan struct {
		Producer
		error
	})

	client.CreateProducerAsync(options, func(producer Producer, err error) {
		c <- struct {
			Producer
			error
		}{producer, err}
		close(c)
	})

	res := <-c
	return res.Producer, res.error
}

func (client *client) CreateProducerAsync(options ProducerOptions, callback func(producer Producer, err error)) {
	createProducerAsync(client, options, callback)
}

func (client *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	c := make(chan struct {
		Consumer
		error
	})

	client.SubscribeAsync(options, func(consumer Consumer, err error) {
		c <- struct {
			Consumer
			error
		}{consumer, err}
		close(c)
	})

	res := <-c
	return res.Consumer, res.error
}

func (client *client) SubscribeAsync(options ConsumerOptions, callback func(Consumer, error)) {
	subscribeAsync(client, options, callback)
}

func (client *client) CreateReader(options ReaderOptions) (Reader, error) {
	c := make(chan struct {
		Reader
		error
	})

	client.CreateReaderAsync(options, func(reader Reader, err error) {
		c <- struct {
			Reader
			error
		}{reader, err}
		close(c)
	})

	res := <-c
	return res.Reader, res.error
}

func (client *client) CreateReaderAsync(options ReaderOptions, callback func(Reader, error)) {
	createReaderAsync(client, options, callback)
}

func (client *client) Close() error {
	res := C.pulsar_client_close(client.ptr)
	if res != C.pulsar_result_Ok {
		return newError(res, "Failed to close Pulsar client")
	} else {
		return nil
	}
}
