package kv

import (
	"context"
	"fmt"
	"github.com/pocockn/pulsar/pulsar-function-go/pb/bookkeeper/kv"
	"github.com/pocockn/pulsar/pulsar-function-go/pb/bookkeeper/kv/rpc"
	"github.com/pocockn/pulsar/pulsar-function-go/pb/bookkeeper/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Table struct {
	cl          rpc.TableServiceClient
	streamProps *stream.StreamProperties
}

func newTable(conn *grpc.ClientConn, streamProps *stream.StreamProperties) *Table {
	return &Table{
		cl:          rpc.NewTableServiceClient(conn),
		streamProps: streamProps,
	}
}

// rtCtx adds the routing information for the given key to the request context.
func (tbl *Table) rtCtx(ctx context.Context, key string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			"bk-rt-sid-bin": int64bin(tbl.streamProps.StreamId),
			"bk-rt-key-bin": key,
		}),
	)
}

func (tbl *Table) get(ctx context.Context, key string) (*kv.KeyValue, error) {
	req := &rpc.RangeRequest{
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
		Key: []byte(key),
	}
	resp, err := tbl.cl.Range(tbl.rtCtx(ctx, key), req)
	if err := errChk(resp, err); err != nil {
		return nil, fmt.Errorf("Range: %v", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0], nil
}

func (tbl *Table) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := tbl.get(ctx, key)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

func (tbl *Table) GetInt(ctx context.Context, key string) (int64, error) {
	v, err := tbl.get(ctx, key)
	if err != nil {
		return 0, err
	}
	if !v.IsNumber {
		return 0, fmt.Errorf("not a number")
	}
	return v.NumberValue, nil
}

func (tbl *Table) Put(ctx context.Context, key string, value []byte) error {
	req := rpc.PutRequest{
		Key:   []byte(key),
		Value: value,
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	return errChk(tbl.cl.Put(tbl.rtCtx(ctx, key), &req))
}

func (tbl *Table) Incr(ctx context.Context, key string, value int64) (int64, error) {
	req := rpc.IncrementRequest{
		Key:      []byte(key),
		Amount:   value,
		GetTotal: true,
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	resp, err := tbl.cl.Increment(tbl.rtCtx(ctx, key), &req)
	if err := errChk(resp, err); err != nil {
		return 0, err
	}
	return resp.TotalAmount, nil
}

func (tbl *Table) Delete(ctx context.Context, key string) error {
	req := rpc.DeleteRangeRequest{
		Key: []byte(key),
		Header: &rpc.RoutingHeader{
			StreamId: tbl.streamProps.StreamId,
			RKey:     []byte(key),
		},
	}
	return errChk(tbl.cl.Delete(tbl.rtCtx(ctx, key), &req))
}
