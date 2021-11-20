module github.com/apache/pulsar/pulsar-function-go/examples

go 1.13

require (
	github.com/apache/pulsar-client-go v0.7.0
	github.com/apache/pulsar/pulsar-function-go v0.0.0
	github.com/datadog/zstd v1.4.6-0.20200617134701-89f69fb7df32 // indirect
	github.com/yahoo/athenz v1.8.55 // indirect
)

replace github.com/apache/pulsar/pulsar-function-go => ../

replace github.com/apache/pulsar/pulsar-function-go/pf => ../pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ../logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ../pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ../conf
