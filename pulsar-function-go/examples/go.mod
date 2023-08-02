module github.com/apache/pulsar/pulsar-function-go/examples

go 1.13

require (
	github.com/apache/pulsar-client-go v0.11.0
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20220120090717-25e59572242e // indirect
	github.com/apache/pulsar/pulsar-function-go v0.0.0
	github.com/beefsack/go-rate v0.0.0-20220214233405-116f4ca011a0 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/spf13/viper v1.8.1 // indirect
)

replace github.com/apache/pulsar/pulsar-function-go => ../

replace github.com/apache/pulsar/pulsar-function-go/pf => ../pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ../logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ../pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ../conf
