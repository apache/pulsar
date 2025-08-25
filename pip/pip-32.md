# PIP-32: Go Function API, Instance and LocalRun

- Status: Adopted
- Author: Xiaolong Ran
- Pull request:
- Mailing list discussion:
- Release:

## Motivation

The server and client sides of the Pulsar function use protobuf for decoupling. In principle, the language supported by protobuf can be supported by the pulsar function. This greatly simplifies our work on developing the go function client, and we don't have to worry about what happens to the runtime and the worker. We treat the instance of the go language to be implemented as the client side, and the two interact through the protobuf protocol. We only need to generate a pb file of the go language according to the pre-defined proto file, and encapsulate a go function client on the basis of this, and expose it to external users. Before we provided the Java and python client of the pulsar function to the community, but as the go community grows, there is more and more demand for the go function client, so we urgently need the go function client, one of which is the client that enriches the pulsar function. Second, to facilitate people familiar with the go language, quickly understand and use the pulsar function.
## Goals

Implement a MVP (Minimum Viable Product) of Go Functions, which includes Go Function API, and Go Instance Implementation. And be able to localrun a go function.

## None Goals

Following areas are not considered for the first implementation of Go Function. Each of them itself can be considered a separated project.

- Metrics
- Admin Operations
- State
- Schema
- Secrets
- Log Topic
- thentication & Authorization
## API

### Context api

```
type FunctionContext struct {
	InstanceConf *InstanceConf
	UserConfigs  map[string]interface{}
	InputTopics  []string
	StartTime    time.Time
	Producer     pulsar.Producer
}

func NewFuncContext(conf *InstanceConf, client pulsar.Client, inTopics []string) *FunctionContext {}

func (c *FunctionContext) GetInstanceID() int {
	return c.InstanceConf.InstanceID
}

func (c *FunctionContext) GetInputTopics() []string {
	return c.InputTopics
}

func (c *FunctionContext) GetOutputTopic() string {
	return c.InstanceConf.FuncDetails.GetSink().Topic
}

func (c *FunctionContext) GetFuncTenant() string {
	return c.InstanceConf.FuncDetails.Tenant
}

func (c *FunctionContext) GetFuncName() string {
	return c.InstanceConf.FuncDetails.Name
}

func (c *FunctionContext) GetFuncNamespace() string {
	return c.InstanceConf.FuncDetails.Namespace
}

func (c *FunctionContext) GetFuncID() string {
	return c.InstanceConf.FuncID
}

func (c *FunctionContext) GetFuncVersion() string {
	return c.InstanceConf.FuncVersion
}

func (c *FunctionContext) GetUserConfValue(key string) interface{} {
	return c.UserConfigs[key]
}

func (c *FunctionContext) GetUserConfMap() map[string]interface{} {
	return c.UserConfigs
}

type key struct{}

var contextKey = &key{}

func NewContext(parent context.Context, fc *FunctionContext) context.Context {
	return context.WithValue(parent, contextKey, fc)
}

func FromContext(ctx context.Context) (*FunctionContext, bool) {
	fc, ok := ctx.Value(contextKey).(*FunctionContext)
	return fc, ok
}
```

### Instance api


We will introduce a new public API named `pulsarfunc.Start(funcName interface{})`, Start receives the name of a function. (For details on why we choose this approach, please checkout Section `proposed changes` and Section `alternatives`.)

```

func Start(funcName interface{}) {}

```
#### Rules:

- function must be a function type
- function may take between 0 and two arguments.
- if there are two arguments, the first argument must satisfy the "context.Context" interface.
- function may return between 0 and two arguments.
- if there are two return values, the second argument must be an error.
- if there is one return value it must be an error.

#### Valid function signatures:

- func ()
- func () error
- func (input) error
- func () (output, error)
- func (input) (output, error)
- func (context.Context) error
- func (context.Context, input) error
- func (context.Context) (output, error)
- func (context.Context, input) (output, error)


## Example

Below is an example on showing how to use go pulsar function API.

```
package main

import (
	"fmt"

	pulsarfunc "github.com/apache/pulsar/pulsar-function-go"
)

func hello() {
	fmt.Println("hello pulsar function")
}

func main(){
	pulsarfunc.Start(hello)
}
```

## Proposed Changes
### Instance

Different from python and java, Go is a “static-linking” language. Although it recently supports dynamic linking via `plugins` (see `Alternatives`). Most of the go developers expect to compile a pulsar library with the function he/she develope together, to produce a single executable binary.

So in the implementation of Pulsar go function, we choose a different approach than Java and Python. We offered a `instance` API for go function developer. 

The `instance` API provides the ability to start a go function. See section `instance api`.

In the Instance API, the user only to pass in the name of the function he/she writes (as shown in the example). In the implementation, we use reflection to verify the parameters and returns values according to the received function name. After we successfully verified the function, the implementation will create a pulsar client, setup the consumer to receive messages from the input topics and setup the produce to produce results to the output topics, and the instance will invoke the function each time when it receives a message.

## Testing Plan

In order to ensure the correctness of the new features, the corresponding unit test and integration test will be added to verify.

## Alternatives
The Go plugin was introduced in Go version 1.8, which makes it possible to use the plugin to implement the go function client. Plugins can create new types of Go systems, take advantage of the late binding nature of dynamically shared object binaries (such as Gosh), or push plug-in binaries to a node's distributed system as needed, or complete a containerized system at runtime. and many more

The plugin system is not perfect. There are still defects, as follows:
- Bring certain security risks, if some illegal modules are injected, how to prevent them
- Bring some unstable factors to the system. If there is a problem with the module, it will not lead to a service crash.
- It brings problems to version management, especially in today's microservices, the same service, loaded with different plugins, how to manage the version, how should the plugin version be managed
- The go1.8 version plugin feature is only available on Linux. go1.10 can also be run on a Mac.
