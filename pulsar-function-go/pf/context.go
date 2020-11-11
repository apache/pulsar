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

package pf

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type FunctionContext struct {
	instanceConf  *instanceConf
	userConfigs   map[string]interface{}
	logAppender   *LogAppender
	outputMessage func(topic string) pulsar.Producer
	record        pulsar.Message
}

func NewFuncContext() *FunctionContext {
	instanceConf := newInstanceConf()
	userConfigs := buildUserConfig(instanceConf.funcDetails.GetUserConfig())

	fc := &FunctionContext{
		instanceConf: instanceConf,
		userConfigs:  userConfigs,
	}
	return fc
}

func (c *FunctionContext) GetInstanceID() int {
	return c.instanceConf.instanceID
}

func (c *FunctionContext) GetInputTopics() []string {
	inputMap := c.instanceConf.funcDetails.GetSource().InputSpecs
	inputTopics := make([]string, len(inputMap))
	i := 0
	for k := range inputMap {
		inputTopics[i] = k
		i++
	}
	return inputTopics
}

func (c *FunctionContext) GetOutputTopic() string {
	return c.instanceConf.funcDetails.GetSink().Topic
}

func (c *FunctionContext) GetTenantAndNamespace() string {
	return c.GetFuncTenant() + "/" + c.GetFuncNamespace()
}

func (c *FunctionContext) GetTenantAndNamespaceAndName() string {
	return c.GetFuncTenant() + "/" + c.GetFuncNamespace() + "/" + c.GetFuncName()
}

func (c *FunctionContext) GetFuncTenant() string {
	return c.instanceConf.funcDetails.Tenant
}

func (c *FunctionContext) GetFuncName() string {
	return c.instanceConf.funcDetails.Name
}

func (c *FunctionContext) GetFuncNamespace() string {
	return c.instanceConf.funcDetails.Namespace
}

func (c *FunctionContext) GetFuncID() string {
	return c.instanceConf.funcID
}

func (c *FunctionContext) GetPort() int {
	return c.instanceConf.port
}

func (c *FunctionContext) GetClusterName() string {
	return c.instanceConf.clusterName
}

func (c *FunctionContext) GetExpectedHealthCheckInterval() int32 {
	return c.instanceConf.expectedHealthCheckInterval
}
func (c *FunctionContext) GetExpectedHealthCheckIntervalAsDuration() time.Duration {
	return time.Duration(c.instanceConf.expectedHealthCheckInterval)
}

func (c *FunctionContext) GetMaxIdleTime() int64 {
	return int64(c.GetExpectedHealthCheckIntervalAsDuration() * 3 * time.Second)
}

func (c *FunctionContext) GetFuncVersion() string {
	return c.instanceConf.funcVersion
}

func (c *FunctionContext) GetUserConfValue(key string) interface{} {
	return c.userConfigs[key]
}

func (c *FunctionContext) GetUserConfMap() map[string]interface{} {
	return c.userConfigs
}

// NewOutputMessage send message to the topic
// @param topicName: The name of the topic for output message
func (c *FunctionContext) NewOutputMessage(topicName string) pulsar.Producer {
	return c.outputMessage(topicName)
}

// SetCurrentRecord sets the current message into the function context
// called for each message before executing a handler function
func (c *FunctionContext) SetCurrentRecord(record pulsar.Message) {
	c.record = record
}

// GetCurrentRecord gets the current message from the function context
func (c *FunctionContext) GetCurrentRecord() pulsar.Message {
	return c.record

}

// An unexported type to be used as the key for types in this package.
// This prevents collisions with keys defined in other packages.
type key struct{}

// contextKey is the key for FunctionContext values in context.Context.
// It is unexported;
// clients should use FunctionContext.NewContext and FunctionContext.FromContext
// instead of using this key directly.
var contextKey = &key{}

// NewContext returns a new Context that carries value u.
func NewContext(parent context.Context, fc *FunctionContext) context.Context {
	return context.WithValue(parent, contextKey, fc)
}

// FromContext returns the User value stored in ctx, if any.
func FromContext(ctx context.Context) (*FunctionContext, bool) {
	fc, ok := ctx.Value(contextKey).(*FunctionContext)
	return fc, ok
}

func buildUserConfig(data string) map[string]interface{} {
	m := make(map[string]interface{})

	json.Unmarshal([]byte(data), &m)

	return m
}
