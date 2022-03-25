//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package conf

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"time"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	"gopkg.in/yaml.v2"
)

const ConfigPath = "conf/conf.yaml"

type Conf struct {
	PulsarServiceURL string        `json:"pulsarServiceURL" yaml:"pulsarServiceURL"`
	InstanceID       int           `json:"instanceID" yaml:"instanceID"`
	FuncID           string        `json:"funcID" yaml:"funcID"`
	FuncVersion      string        `json:"funcVersion" yaml:"funcVersion"`
	MaxBufTuples     int           `json:"maxBufTuples" yaml:"maxBufTuples"`
	Port             int           `json:"port" yaml:"port"`
	ClusterName      string        `json:"clusterName" yaml:"clusterName"`
	KillAfterIdleMs  time.Duration `json:"killAfterIdleMs" yaml:"killAfterIdleMs"`
	// function details config
	Tenant               string `json:"tenant" yaml:"tenant"`
	NameSpace            string `json:"nameSpace" yaml:"nameSpace"`
	Name                 string `json:"name" yaml:"name"`
	LogTopic             string `json:"logTopic" yaml:"logTopic"`
	ProcessingGuarantees int32  `json:"processingGuarantees" yaml:"processingGuarantees"`
	SecretsMap           string `json:"secretsMap" yaml:"secretsMap"`
	Runtime              int32  `json:"runtime" yaml:"runtime"`
	AutoACK              bool   `json:"autoAck" yaml:"autoAck"`
	Parallelism          int32  `json:"parallelism" yaml:"parallelism"`
	//source config
	SubscriptionType     int32  `json:"subscriptionType" yaml:"subscriptionType"`
	TimeoutMs            uint64 `json:"timeoutMs" yaml:"timeoutMs"`
	SubscriptionName     string `json:"subscriptionName" yaml:"subscriptionName"`
	CleanupSubscription  bool   `json:"cleanupSubscription"  yaml:"cleanupSubscription"`
	SubscriptionPosition int32  `json:"subscriptionPosition" yaml:"subscriptionPosition"`
	//source input specs
	SourceSpecTopic            string `json:"sourceSpecsTopic" yaml:"sourceSpecsTopic"`
	SourceSchemaType           string `json:"sourceSchemaType" yaml:"sourceSchemaType"`
	IsRegexPatternSubscription bool   `json:"isRegexPatternSubscription" yaml:"isRegexPatternSubscription"`
	ReceiverQueueSize          int32  `json:"receiverQueueSize" yaml:"receiverQueueSize"`
	//sink spec config
	SinkSpecTopic  string `json:"sinkSpecsTopic" yaml:"sinkSpecsTopic"`
	SinkSchemaType string `json:"sinkSchemaType" yaml:"sinkSchemaType"`
	//resources config
	Cpu  float64 `json:"cpu" yaml:"cpu"`
	Ram  int64   `json:"ram" yaml:"ram"`
	Disk int64   `json:"disk" yaml:"disk"`
	//retryDetails config
	MaxMessageRetries           int32  `json:"maxMessageRetries" yaml:"maxMessageRetries"`
	DeadLetterTopic             string `json:"deadLetterTopic" yaml:"deadLetterTopic"`
	ExpectedHealthCheckInterval int32  `json:"expectedHealthCheckInterval" yaml:"expectedHealthCheckInterval"`
	UserConfig                  string `json:"userConfig" yaml:"userConfig"`
	//metrics config
	MetricsPort int `json:"metricsPort" yaml:"metricsPort"`
}

var (
	help         bool
	confFilePath string
	confContent  string
)

func (c *Conf) GetConf() *Conf {
	flag.Parse()

	if help {
		flag.Usage()
	}

	if confContent == "" && confFilePath == "" {
		log.Errorf("no yaml file or conf content provided")
		return nil
	}

	if confFilePath != "" {
		yamlFile, err := ioutil.ReadFile(confFilePath)
		if err == nil {
			err = yaml.Unmarshal(yamlFile, c)
			if err != nil {
				log.Errorf("unmarshal yaml file error:%s", err.Error())
				return nil
			}
		} else if os.IsNotExist(err) && confContent == "" {
			log.Errorf("conf file not found, no config content provided, err:%s", err.Error())
			return nil
		} else if !os.IsNotExist(err) {
			log.Errorf("load conf file failed, err:%s", err.Error())
			return nil
		}
	}

	if confContent != "" {
		err := json.Unmarshal([]byte(confContent), c)
		if err != nil {
			log.Errorf("unmarshal config content error:%s", err.Error())
			return nil
		}
	}

	return c
}

func init() {
	var defaultPath string
	if err := os.Chdir("../"); err == nil {
		defaultPath = ConfigPath
	}
	log.Infof("The default config file path is: %s", defaultPath)

	flag.BoolVar(&help, "help", false, "print help cmd")
	flag.StringVar(&confFilePath, "instance-conf-path", defaultPath, "config conf.yml filepath")
	flag.StringVar(&confContent, "instance-conf", "", "the string content of Conf struct")
}
