module github.com/apache/pulsar/pulsar-function-go/tests

go 1.13

require (
	github.com/apache/pulsar-client-go v0.0.0-20200116214305-4d788d9935ed
	github.com/apache/pulsar/pulsar-function-go/core v0.0.0
)

replace github.com/apache/pulsar/pulsar-function-go/core => ../core

replace github.com/apache/pulsar/pulsar-function-go/core/pf => ../core/pf

replace github.com/apache/pulsar/pulsar-function-go/core/logutil => ../core/logutil

replace github.com/apache/pulsar/pulsar-function-go/core/pb => ../core/pb

replace github.com/apache/pulsar/pulsar-function-go/core/conf => ../core/conf
