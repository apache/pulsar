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

package pf

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

func getProperties(fullyQualifiedName string, instanceID int) map[string]string {
	propertiesMap := make(map[string]string)
	propertiesMap["application"] = "pulsar-function"
	propertiesMap["id"] = fullyQualifiedName
	propertiesMap["instance_id"] = fmt.Sprintf("%d", instanceID)

	return propertiesMap
}

func getDefaultSubscriptionName(tenant, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", tenant, namespace, name)
}

func getFullyQualifiedInstanceID(tenant, namespace, name string, instanceID int) string {
	return fmt.Sprintf("%s/%s/%s:%d", tenant, namespace, name, instanceID)
}

func messageIDStr(msg pulsar.Message) string {
	// <ledger ID>:<entry ID>:<partition index>:<batch index>
	return fmt.Sprintf("%d:%d:%d:%d",
		msg.ID().LedgerID(),
		msg.ID().EntryID(),
		msg.ID().PartitionIdx(),
		msg.ID().BatchIdx())
}
