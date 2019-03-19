/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package conf

import (
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"os/user"
	"time"

	"github.com/apache/pulsar/pulsar-function-go/log"
	"gopkg.in/yaml.v2"
)

const ConfigPath = "github.com/apache/pulsar/pulsar-function-go/conf/conf.yaml"

type Conf struct {
	PulsarServiceURL string        `yaml:"pulsarServiceURL"`
	InputTopics      []string      `yaml:"intopics"`
	InstanceID       int           `yaml:"instanceID"`
	FuncID           string        `yaml:"funcID"`
	FuncVersion      string        `yaml:"funcVersion"`
	Name             string        `yaml:"name"`
	MaxBufTuples     int           `yaml:"maxBufTuples"`
	Port             int           `yaml:"port"`
	ClusterName      string        `yaml:"clusterName"`
	IsRegexPattern   bool          `yaml:"isRegexPattern"`
	ReceiverQueueVal int32         `yaml:"receiverQueueVal"`
	InputSpecsTopic  string        `yaml:"inputSpecs"`
	AutoACK          bool          `yaml:"autoAck"`
	SinkSpec         string        `yaml:"sinkSpec"`
	KillAfterIdleMs  time.Duration `yaml:"killAfterIdleMs"`
}

var opts string

func (c *Conf) GetConf() *Conf {
	flag.Parse()

	yamlFile, err := ioutil.ReadFile(opts)
	if err != nil {
		log.Errorf("not found conf file, err:%s", err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Errorf("unmarshal yaml file error:%s", err.Error())
	}
	return c
}

func (c *Conf) Verify() error {
	if c == nil {
		return errors.New("config file is nil")
	}
	return nil
}

func init() {
	var homeDir string
	usr, err := user.Current()
	if err == nil {
		homeDir = usr.HomeDir
	}

	// Fall back to standard HOME environment variable that works
	// for most POSIX OSes if the directory from the Go standard
	// lib failed.
	if err != nil || homeDir == "" {
		homeDir = os.Getenv("HOME")
	}
	defaultPath := homeDir + "/" + ConfigPath
	flag.StringVar(&opts, "path", defaultPath, "config conf.yml filepath")
}
