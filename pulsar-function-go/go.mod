module github.com/apache/pulsar/pulsar-function-go

go 1.13

require (
	github.com/apache/pulsar-client-go v0.1.0
	github.com/golang/protobuf v1.3.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.26.0
	gopkg.in/yaml.v2 v2.2.8
)

replace github.com/apache/pulsar/pulsar-function-go/pf => ./pf

replace github.com/apache/pulsar/pulsar-function-go/logutil => ./logutil

replace github.com/apache/pulsar/pulsar-function-go/pb => ./pb

replace github.com/apache/pulsar/pulsar-function-go/conf => ./conf
