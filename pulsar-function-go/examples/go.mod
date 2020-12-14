module github.com/apache/pulsar/pulsar-function-go/examples

go 1.13

require (
	github.com/apache/pulsar-client-go v0.3.1-0.20201201083639-154bff0bb825
	github.com/apache/pulsar/pulsar-function-go v0.0.0
)

replace github.com/apache/pulsar/pulsar-function-go => ../

replace github.com/apache/pulsar/pulsar-function-go/pf => ../pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ../logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ../pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ../conf
