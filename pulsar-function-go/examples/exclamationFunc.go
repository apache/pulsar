package main

import (
	"context"
	"fmt"

	"github.com/apache/pulsar/pulsar-function-go/examples/util"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func hello(ctx context.Context) {
	if fc, ok := pf.FromContext(ctx); ok {
		fmt.Printf("function ID is:%s=====", fc.GetFuncID())
		fmt.Printf("function version is:%s\n", fc.GetFuncVersion())
		fmt.Println()
	}
}

func main() {
	util.SetProducer()
	pf.Start(hello)
}
