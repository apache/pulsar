module github.com/apache/pulsar/pulsar-function-go

go 1.13

require (
	github.com/apache/pulsar-client-go v0.8.1
	github.com/golang/protobuf v1.5.3
	github.com/prometheus/client_golang v1.12.2
	github.com/prometheus/client_model v0.3.0
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.8.3
	google.golang.org/grpc v1.56.3
	google.golang.org/protobuf v1.30.0
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/apache/pulsar/pulsar-function-go/pf => ./pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ./logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ./pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ./conf
