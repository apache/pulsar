module github.com/apache/pulsar/pulsar-function-go/core

go 1.13

require (
	github.com/apache/pulsar-client-go v0.0.0-20200116214305-4d788d9935ed
	github.com/golang/protobuf v1.3.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.26.0
	gopkg.in/yaml.v2 v2.2.8
)

replace github.com/apache/pulsar/pulsar-function-go/core/pf => ./pf

replace github.com/apache/pulsar/pulsar-function-go/core/logutil => ./logutil

replace github.com/apache/pulsar/pulsar-function-go/core/pb => ./pb

replace github.com/apache/pulsar/pulsar-function-go/core/conf => ./conf
