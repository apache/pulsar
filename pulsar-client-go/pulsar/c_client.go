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
	"strings"
	"unsafe"

	log "github.com/apache/pulsar/pulsar-client-go/logutil"
)

//export pulsarClientLoggerProxy
func pulsarClientLoggerProxy(level C.pulsar_logger_level_t, file *C.char, line C.int, message *C.char, ctx unsafe.Pointer) {
	logger := restorePointerNoDelete(ctx).(func(log.LoggerLevel, string, int, string))

	logger(log.LoggerLevel(level), C.GoString(file), int(line), C.GoString(message))
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
		options.Logger = func(level log.LoggerLevel, file string, line int, message string) {
			log.Infof("%-5s | %s:%d | %s", level, file, line, message)
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

	if options.TLSValidateHostname {
		C.pulsar_client_configuration_set_validate_hostname(conf, cBool(options.TLSValidateHostname))
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

func newAuthenticationToken(token string) Authentication {
	cToken := C.CString(token)
	defer C.free(unsafe.Pointer(cToken))

	auth := &authentication{
		ptr: C.pulsar_authentication_token_create(cToken),
	}

	runtime.SetFinalizer(auth, authenticationFinalizer)
	return auth
}

//export pulsarClientTokenSupplierProxy
func pulsarClientTokenSupplierProxy(ctx unsafe.Pointer) *C.char {
	tokenSupplier := restorePointerNoDelete(ctx).(func() string)
	token := tokenSupplier()
	// The C string will be freed from within the C wrapper itself
	return C.CString(token)
}

func newAuthenticationTokenSupplier(tokenSupplier func() string) Authentication {
	supplierPtr := savePointer(tokenSupplier)

	auth := &authentication{
		ptr: C._pulsar_authentication_token_create_with_supplier(supplierPtr),
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
	ptr  *C.pulsar_client_t
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
	}, 1)

	client.CreateProducerAsync(options, nil, func(producer Producer, err error) {
		c <- struct {
			Producer
			error
		}{producer, err}
		close(c)
	})

	res := <-c
	return res.Producer, res.error
}

func (client *client) CreateProducerWithSchema(options ProducerOptions, schema Schema) (Producer, error) {
	// Create is implemented on async create with a channel to wait for
	// completion without blocking the real thread
	c := make(chan struct {
		Producer
		error
	}, 1)

	client.CreateProducerAsync(options, schema, func(producer Producer, err error) {
		c <- struct {
			Producer
			error
		}{producer, err}
		close(c)
	})

	res := <-c
	return res.Producer, res.error
}

func (client *client) CreateProducerAsync(options ProducerOptions, schema Schema, callback func(producer Producer, err error)) {
	createProducerAsync(client, schema, options, callback)
}

func (client *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	c := make(chan struct {
		Consumer
		error
	}, 1)

	client.SubscribeAsync(options, nil, func(consumer Consumer, err error) {
		c <- struct {
			Consumer
			error
		}{consumer, err}
		close(c)
	})

	res := <-c
	return res.Consumer, res.error
}

func (client *client) SubscribeWithSchema(options ConsumerOptions, schema Schema) (Consumer, error) {
	c := make(chan struct {
		Consumer
		error
	}, 1)

	client.SubscribeAsync(options, schema, func(consumer Consumer, err error) {
		c <- struct {
			Consumer
			error
		}{consumer, err}
		close(c)
	})

	res := <-c
	return res.Consumer, res.error
}

func (client *client) SubscribeAsync(options ConsumerOptions, schema Schema, callback func(Consumer, error)) {
	subscribeAsync(client, options, schema, callback)
}

func (client *client) CreateReader(options ReaderOptions) (Reader, error) {
	c := make(chan struct {
		Reader
		error
	}, 1)

	client.CreateReaderAsync(options, nil, func(reader Reader, err error) {
		c <- struct {
			Reader
			error
		}{reader, err}
		close(c)
	})

	res := <-c
	return res.Reader, res.error
}

func (client *client) CreateReaderWithSchema(options ReaderOptions, schema Schema) (Reader, error) {
	c := make(chan struct {
		Reader
		error
	}, 1)

	client.CreateReaderAsync(options, schema, func(reader Reader, err error) {
		c <- struct {
			Reader
			error
		}{reader, err}
		close(c)
	})

	res := <-c
	return res.Reader, res.error
}

//export pulsarGetTopicPartitionsCallbackProxy
func pulsarGetTopicPartitionsCallbackProxy(res C.pulsar_result, cPartitions *C.pulsar_string_list_t, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func([]string, error))

	if res != C.pulsar_result_Ok {
		callback(nil, newError(res, "Failed to get partitions for topic"))
	} else {
		numPartitions := int(C.pulsar_string_list_size(cPartitions))
		partitions := make([]string, numPartitions)
		for i := 0; i < numPartitions; i++ {
			partitions[i] = C.GoString(C.pulsar_string_list_get(cPartitions, C.int(i)))
		}

		C.pulsar_string_list_free(cPartitions)

		callback(partitions, nil)
	}
}

func (client *client) CreateReaderAsync(options ReaderOptions, schema Schema, callback func(Reader, error)) {
	createReaderAsync(client, schema, options, callback)
}

func (client *client) TopicPartitions(topic string) ([]string, error) {
	c := make(chan struct {
		partitions []string
		err        error
	}, 1)

	topicPartitionsAsync(client, topic, func(partitions []string, err error) {
		c <- struct {
			partitions []string
			err        error
		}{partitions, err}
		close(c)
	})

	res := <-c
	return res.partitions, res.err
}

type getPartitionsCallback struct {
	partitions []string
	channel    chan ReaderMessage
}

func topicPartitionsAsync(client *client, topic string, callback func([]string, error)) {
	if topic == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "topic is required"))
		return
	}

	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))

	C._pulsar_client_get_topic_partitions(client.ptr, cTopic, savePointer(callback))
}

func (client *client) Close() error {
	res := C.pulsar_client_close(client.ptr)
	if res != C.pulsar_result_Ok {
		return newError(res, "Failed to close Pulsar client")
	} else {
		return nil
	}
}
