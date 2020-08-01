module github.com/apache/pulsar/pulsar-function-go/tests

go 1.13

require (
	github.com/apache/pulsar-client-go v0.1.0
	github.com/apache/pulsar/pulsar-function-go v0.0.0
)

replace github.com/apache/pulsar/pulsar-function-go => ../

replace github.com/apache/pulsar/pulsar-function-go/pf => ../pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ../logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ../pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ../conf
