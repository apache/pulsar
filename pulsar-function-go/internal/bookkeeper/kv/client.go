package kv

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/pulsar/pulsar-function-go/pb/bookkeeper/kv/rpc"
	"github.com/apache/pulsar/pulsar-function-go/pb/bookkeeper/storage"
	"github.com/apache/pulsar/pulsar-function-go/pb/bookkeeper/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net/url"
)

// https://github.com/apache/bookkeeper/blob/release-4.10.0/stream/clients/python/bookkeeper/common/constants.py#L18
var (
	rootRangeID       = 0
	rootRangeMetadata = metadata.New(map[string]string{
		"bk-rt-sc-id-bin": int64bin(int64(rootRangeID)),
	})
)

func int64bin(v int64) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return string(buf[:])
}

type respHdr interface {
	GetHeader() *rpc.ResponseHeader
}

type StorageError storage.StatusCode

func (se StorageError) Code() storage.StatusCode {
	return storage.StatusCode(se)
}

func (se StorageError) Error() string {
	return se.Code().String()
}

func errChk(resp respHdr, err error) error {
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("empty response")
	}
	hdr := resp.GetHeader()
	if hdr.Code == storage.StatusCode_SUCCESS {
		return nil
	}
	return StorageError(hdr.Code)
}

type StorageClientSettings struct {
	ServiceURI url.URL
}

type Client struct {
	scs       StorageClientSettings
	conn      *grpc.ClientConn
	namespace string
}

func NewClient(ctx context.Context, addr, namespace string) (*Client, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("connecting to bookkeeper: %v", err)
	}

	cl := &Client{
		conn:      conn,
		namespace: namespace,
	}

	return cl, nil
}

func (cl *Client) GetTable(ctx context.Context, name string) (*Table, error) {
	req := &storage.GetStreamRequest{
		Id: &storage.GetStreamRequest_StreamName{StreamName: &stream.StreamName{
			NamespaceName: cl.namespace,
			StreamName:    name,
		}},
	}

	rootCl := storage.NewRootRangeServiceClient(cl.conn)
	resp, err := rootCl.GetStream(
		metadata.NewOutgoingContext(ctx, rootRangeMetadata),
		req)
	if err != nil {
		return nil, fmt.Errorf("GetStream: %v", err)
	}
	if resp.Code != storage.StatusCode_SUCCESS {
		return nil, fmt.Errorf("retrieving stream: %v", StorageError(resp.Code))
	}

	return newTable(cl.conn, resp.StreamProps), nil
}

func (cl *Client) CreateTable(ctx context.Context, name string, conf *stream.StreamConfiguration) (*Table, error) {
	req := &storage.CreateStreamRequest{
		NsName:     cl.namespace,
		Name:       name,
		StreamConf: conf,
	}

	rootCl := storage.NewRootRangeServiceClient(cl.conn)
	resp, err := rootCl.CreateStream(
		metadata.NewOutgoingContext(ctx, rootRangeMetadata),
		req)
	if err != nil {
		return nil, fmt.Errorf("CreateStream: %v", err)
	}
	if resp.Code != storage.StatusCode_SUCCESS {
		return nil, fmt.Errorf("creating stream: %v", StorageError(resp.Code))
	}

	return newTable(cl.conn, resp.StreamProps), nil
}
