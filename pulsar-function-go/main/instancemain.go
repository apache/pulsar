package main

import (
	"flag"

	"github.com/apache/pulsar/pulsar-function-go/instance"
)

var args = struct {
	funcDetails       string
	instanceId        int
	funcID            string
	funcVersion       string
	pulsarServiceURL  string
	port              int
	maxBufferedTuples string
	logDirectory      string
	logFile           string
	logConfFile       string
	clusterName       string
	//clientAuthPlugin            string
	//clientAuthParams            string
	//useTLS                      string
	//tlsAllowInsecureConn        bool
	//hostnameVerificationEnabled bool
	//tlsTrustCertPath            string
	//metricsPort                 int
	//expectedHealthCheckInterval int
	//secretsProvider             string
	//secretsProviderConfig       string
	//installUsercodeDependencies string
	//dependencyRepository        string
	//extraDependencyRepository   string
	//stateStorageServiceURL      string
}{
	funcDetails: "",
}

func main() {
	flag.StringVar(&args.funcDetails, "--function_details", args.funcDetails, "Function Details Json String")
	flag.IntVar(&args.instanceId, "--instance_id", args.instanceId, "Instance Id")
	flag.StringVar(&args.funcID, "--function_id", args.funcID, "Function Id")
	flag.StringVar(&args.funcVersion, "--function_version", args.funcVersion, "Function Version")
	flag.StringVar(&args.pulsarServiceURL, "--pulsar_serviceurl", args.pulsarServiceURL, "Pulsar Service Url")
	flag.IntVar(&args.port, "--port", args.port, "Instance Port")
	flag.StringVar(&args.clusterName, "--clusterName", args.clusterName, "The name of the cluster this instance is running on")

	flag.Parse()

	instance.Start()
}
