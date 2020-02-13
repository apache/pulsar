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
	"context"
	"fmt"
	"net"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type InstanceControlServicer struct {
	goInstance *goInstance
}

func (icServicer *InstanceControlServicer) GetFunctionStatus(
	ctx context.Context, req *empty.Empty) (*pb.FunctionStatus, error) {
	return icServicer.goInstance.getFunctionStatus(), nil
	//return nil, status.Errorf(codes.Unimplemented, "method GetFunctionStatus not implemented")
}
func (icServicer *InstanceControlServicer) GetAndResetMetrics(
	ctx context.Context, req *empty.Empty) (*pb.MetricsData, error) {
	return icServicer.goInstance.getAndResetMetrics(), nil
}
func (icServicer *InstanceControlServicer) ResetMetrics(
	ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return icServicer.goInstance.resetMetrics(), nil
}
func (icServicer *InstanceControlServicer) GetMetrics(
	ctx context.Context, req *empty.Empty) (*pb.MetricsData, error) {
	return icServicer.goInstance.getMetrics(), nil
}
func (icServicer *InstanceControlServicer) HealthCheck(
	ctx context.Context, req *empty.Empty) (*pb.HealthCheckResult, error) {
	return icServicer.goInstance.healthCheck(), nil
}

func (icServicer *InstanceControlServicer) serve(goInstance *goInstance) *grpc.Server {
	// create a listener on TCP port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", goInstance.context.GetPort()))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// create a gRPC server object
	grpcServer := grpc.NewServer()
	// must register before we start the service.
	pb.RegisterInstanceControlServer(grpcServer, icServicer)
	// start the server
	log.Infof("Serving InstanceCommunication on port %d", goInstance.context.GetPort())
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	return grpcServer
}
