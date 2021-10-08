module github.com/apache/pulsar/pulsar-function-go

go 1.13

require (
	github.com/apache/pulsar-client-go v0.6.0
	github.com/datadog/zstd v1.4.6-0.20200617134701-89f69fb7df32 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.5.1
	github.com/yahoo/athenz v1.8.55 // indirect
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/apache/pulsar/pulsar-function-go/pf => ./pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ./logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ./pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ./conf
