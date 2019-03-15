package main

import (
	"fmt"

	"github.com/apache/pulsar/pulsar-function-go/examples/util"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func world() {
	fmt.Println("hello pulsar function")
}

func main() {
	util.SetProducer()
	pf.Start(world)
}
