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
#cgo LDFLAGS: -lpulsar
#include "c_go_pulsar.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
	"log"
	"strings"
)

//export pulsarClientLoggerProxy
func pulsarClientLoggerProxy(level C.pulsar_logger_level_t, file *C.char, line C.int, message *C.char, ctx unsafe.Pointer) {
	logger := restorePointerNoDelete(ctx).(func(LoggerLevel, string, int, string))

	logger(LoggerLevel(level), C.GoString(file), int(line), C.GoString(message))
}

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

	if options.Logger == nil {
		// Configure a default logger with same date format as Go logs
		options.Logger = func(level LoggerLevel, file string, line int, message string) {
			log.Printf("%-5s | %s:%d | %s", level, file, line, message)
		}
	}

	C._pulsar_client_configuration_set_logger(conf, savePointer(options.Logger))

	// If service url is on encrypted protocol, enable TLS
	if strings.HasPrefix(options.URL, "pulsar+ssl://") || strings.HasPrefix(options.URL, "https://") {
		C.pulsar_client_configuration_set_use_tls(conf, cBool(true))
	}

	if options.Authentication != nil {
		C.pulsar_client_configuration_set_auth(conf, options.Authentication.(*authentication).ptr)
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

	url := C.CString(options.URL)
	defer C.free(unsafe.Pointer(url))

	client := &client{
		ptr: C.pulsar_client_create(url, conf),
	}

	if options.Authentication != nil {
		client.auth = options.Authentication.(*authentication)
	}

	C.pulsar_client_configuration_free(conf)
	runtime.SetFinalizer(client, clientFinalizer)
	return client, nil
}

type authentication struct {
	ptr *C.pulsar_authentication_t
}

func newAuthenticationTLS(certificatePath string, privateKeyPath string) Authentication {
	cCertificatePath := C.CString(certificatePath)
	cPrivateKeyPath := C.CString(privateKeyPath)
	defer C.free(unsafe.Pointer(cCertificatePath))
	defer C.free(unsafe.Pointer(cPrivateKeyPath))

	auth := &authentication{
		ptr: C.pulsar_authentication_tls_create(cCertificatePath, cPrivateKeyPath),
	}

	runtime.SetFinalizer(auth, authenticationFinalizer)
	return auth
}

func newAuthenticationAthenz(authParams string) Authentication {
	cAuthParams := C.CString(authParams)
	defer C.free(unsafe.Pointer(cAuthParams))

	auth := &authentication{
		ptr: C.pulsar_authentication_athenz_create(cAuthParams),
	}

	runtime.SetFinalizer(auth, authenticationFinalizer)
	return auth
}

func authenticationFinalizer(authentication *authentication) {
	C.pulsar_authentication_free(authentication.ptr)
}

type client struct {
	ptr *C.pulsar_client_t
	auth *authentication
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
