module github.com/apache/pulsar/pulsar-function-go/tests

go 1.13

require (
	github.com/apache/pulsar-client-go v0.0.0-20200116214305-4d788d9935ed
	github.com/apache/pulsar/pulsar-function-go v0.0.0
)

replace github.com/apache/pulsar/pulsar-function-go => ../

replace github.com/apache/pulsar/pulsar-function-go/pf => ../pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ../logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ../pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ../conf
