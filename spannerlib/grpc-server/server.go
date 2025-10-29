package main

import (
	"context"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"spannerlib/api"
	pb "spannerlib/grpc-server/google/spannerlib/v1"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Missing gRPC server address\n")
	}
	name := os.Args[1]
	tp := "unix"
	if len(os.Args) > 2 {
		tp = os.Args[2]
	}
	//
	if tp == "unix" {
		defer func() { _ = os.Remove(name) }()
		// Set up a channel to listen for OS signals that terminate the process,
		// so we can clean up the temp file in those cases as well.
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			// Wait for a signal.
			<-sigs
			// Delete the temp file.
			_ = os.Remove(name)
			os.Exit(0)
		}()
	}

	lis, err := net.Listen(tp, name)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	grpcServer, err := createServer()
	if err != nil {
		log.Fatalf("failed to create server: %v\n", err)
	}
	log.Printf("Starting gRPC server on %s\n", lis.Addr().String())
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Printf("failed to serve: %v\n", err)
	}
}

func createServer() (*grpc.Server, error) {
	var opts []grpc.ServerOption
	// Set a max message size that is essentially no limit.
	opts = append(opts, grpc.MaxRecvMsgSize(math.MaxInt32))
	grpcServer := grpc.NewServer(opts...)

	server := spannerLibServer{}
	pb.RegisterSpannerLibServer(grpcServer, &server)

	return grpcServer, nil
}

var _ pb.SpannerLibServer = &spannerLibServer{}

type spannerLibServer struct {
	pb.UnimplementedSpannerLibServer
}

func (s *spannerLibServer) Info(_ context.Context, _ *pb.InfoRequest) (*pb.InfoResponse, error) {
	return &pb.InfoResponse{}, nil
}

func (s *spannerLibServer) CreatePool(ctx context.Context, request *pb.CreatePoolRequest) (*pb.Pool, error) {
	id, err := api.CreatePool(ctx, request.ConnectionString)
	if err != nil {
		return nil, err
	}
	return &pb.Pool{Id: id}, nil
}

func (s *spannerLibServer) ClosePool(ctx context.Context, pool *pb.Pool) (*emptypb.Empty, error) {
	err := api.ClosePool(ctx, pool.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) CreateConnection(ctx context.Context, request *pb.CreateConnectionRequest) (*pb.Connection, error) {
	id, err := api.CreateConnection(ctx, request.Pool.Id)
	if err != nil {
		return nil, err
	}
	return &pb.Connection{Pool: request.Pool, Id: id}, nil
}

func (s *spannerLibServer) CloseConnection(ctx context.Context, connection *pb.Connection) (*emptypb.Empty, error) {
	err := api.CloseConnection(ctx, connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func contextWithSameDeadline(ctx context.Context) context.Context {
	newContext := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		// Ignore the returned cancel function here, as the context will be closed when the Rows object is closed.
		//goland:noinspection GoVetLostCancel
		newContext, _ = context.WithDeadline(newContext, deadline)
	}
	return newContext
}

func (s *spannerLibServer) Execute(ctx context.Context, request *pb.ExecuteRequest) (*pb.Rows, error) {
	// Create a new context that is used for the query. We need to do this, because the context that is passed in to
	// this function will be cancelled once the RPC call finishes. That again would cause further calls to Next on the
	// underlying rows object to fail with a 'Context cancelled' error.
	queryContext := contextWithSameDeadline(ctx)
	id, err := api.Execute(queryContext, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	if err != nil {
		return nil, err
	}
	return &pb.Rows{Connection: request.Connection, Id: id}, nil
}

func (s *spannerLibServer) ExecuteStreaming(request *pb.ExecuteRequest, stream grpc.ServerStreamingServer[pb.RowData]) error {
	queryContext := contextWithSameDeadline(stream.Context())
	id, err := api.Execute(queryContext, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	if err != nil {
		return err
	}
	defer func() { _ = api.CloseRows(queryContext, request.Connection.Pool.Id, request.Connection.Id, id) }()
	rows := &pb.Rows{Connection: request.Connection, Id: id}
	metadata, err := api.Metadata(queryContext, request.Connection.Pool.Id, request.Connection.Id, id)
	if err != nil {
		return err
	}
	first := true
	for {
		if row, err := api.Next(queryContext, request.Connection.Pool.Id, request.Connection.Id, id); err != nil {
			return err
		} else {
			if row == nil {
				stats, err := api.ResultSetStats(queryContext, request.Connection.Pool.Id, request.Connection.Id, id)
				if err != nil {
					return err
				}
				res := &pb.RowData{Rows: rows, Stats: stats}
				if first {
					res.Metadata = metadata
					first = false
				}
				if err := stream.Send(res); err != nil {
					return err
				}
				break
			}
			res := &pb.RowData{Rows: rows, Data: []*structpb.ListValue{row}}
			if first {
				res.Metadata = metadata
				first = false
			}
			if err := stream.Send(res); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *spannerLibServer) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	resp, err := api.ExecuteBatch(ctx, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteBatchDmlRequest)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) Metadata(ctx context.Context, rows *pb.Rows) (*spannerpb.ResultSetMetadata, error) {
	metadata, err := api.Metadata(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *spannerLibServer) Next(ctx context.Context, request *pb.NextRequest) (*structpb.ListValue, error) {
	// TODO: Pass in numRows and encoding option.
	values, err := api.Next(ctx, request.Rows.Connection.Pool.Id, request.Rows.Connection.Id, request.Rows.Id)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (s *spannerLibServer) ResultSetStats(ctx context.Context, rows *pb.Rows) (*spannerpb.ResultSetStats, error) {
	stats, err := api.ResultSetStats(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *spannerLibServer) CloseRows(ctx context.Context, rows *pb.Rows) (*emptypb.Empty, error) {
	err := api.CloseRows(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*emptypb.Empty, error) {
	// Create a new context that is used for the transaction. We need to do this, because the context that is passed in
	// to this function will be cancelled once the RPC call finishes. That again would cause further calls on
	// the underlying transaction to fail with a 'Context cancelled' error.
	txContext := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		// Ignore the returned cancel function here, as the context will be closed when the transaction is closed.
		//goland:noinspection GoVetLostCancel
		txContext, _ = context.WithDeadline(txContext, deadline)
	}
	err := api.BeginTransaction(txContext, request.Connection.Pool.Id, request.Connection.Id, request.TransactionOptions)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) Commit(ctx context.Context, connection *pb.Connection) (*spannerpb.CommitResponse, error) {
	resp, err := api.Commit(ctx, connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) Rollback(ctx context.Context, connection *pb.Connection) (*emptypb.Empty, error) {
	err := api.Rollback(ctx, connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) WriteMutations(ctx context.Context, request *pb.WriteMutationsRequest) (*spannerpb.CommitResponse, error) {
	resp, err := api.WriteMutations(ctx, request.Connection.Pool.Id, request.Connection.Id, request.Mutations)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
