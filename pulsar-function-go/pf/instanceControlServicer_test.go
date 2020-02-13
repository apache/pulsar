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
	"log"
	"net"
	"testing"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func getBufDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, url string) (net.Conn, error) {
		return listener.Dial()
	}
}
func TestInstanceControlServicer_serve_creates_valid_instance(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	// create a gRPC server object
	grpcServer := grpc.NewServer()
	instance := newGoInstance()
	servicer := InstanceControlServicer{instance}
	// must register before we start the service.
	pb.RegisterInstanceControlServer(grpcServer, &servicer)
	// start the server
	log.Printf("Serving InstanceCommunication on port %d", instance.context.GetPort())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	// Now we can setup the client:

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(getBufDialer(lis)), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewInstanceControlClient(conn)
	resp, err := client.HealthCheck(ctx, &empty.Empty{})
	if err != nil {
		t.Fatalf("SayHello failed: %v", err)
	}

	// Test for output.
	log.Printf("Response: %+v", resp.Success)
	assert.Equal(t, resp.Success, true)
}
