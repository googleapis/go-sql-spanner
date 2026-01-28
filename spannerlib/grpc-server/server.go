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
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
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
	id, err := api.CreatePool(ctx, request.UserAgentSuffix, request.ConnectionString)
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

func (s *spannerLibServer) Execute(ctx context.Context, request *pb.ExecuteRequest) (returnedRows *pb.Rows, returnedErr error) {
	// Only use the context of the gRPC invocation for the DirectExecute option. That is: It is only used
	// for fetching the first results, and can be cancelled after that.
	id, err := api.ExecuteWithDirectExecuteContext(context.Background(), ctx, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	if err != nil {
		return nil, err
	}
	return &pb.Rows{Connection: request.Connection, Id: id}, nil
}

func (s *spannerLibServer) ExecuteStreaming(request *pb.ExecuteRequest, stream grpc.ServerStreamingServer[pb.RowData]) error {
	queryContext := stream.Context()
	id, err := api.Execute(queryContext, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	if err != nil {
		return err
	}
	rows := &pb.Rows{Connection: request.Connection, Id: id}
	return s.streamRows(queryContext, rows, stream)
}

func (s *spannerLibServer) ContinueStreaming(rows *pb.Rows, stream grpc.ServerStreamingServer[pb.RowData]) error {
	queryContext := stream.Context()
	return s.streamRows(queryContext, rows, stream)
}

func (s *spannerLibServer) streamRows(queryContext context.Context, rows *pb.Rows, stream grpc.ServerStreamingServer[pb.RowData]) error {
	defer func() { _ = api.CloseRows(context.Background(), rows.Connection.Pool.Id, rows.Connection.Id, rows.Id) }()
	metadata, err := api.Metadata(queryContext, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return err
	}

	first := true
	for {
		if row, err := api.NextBuffered(queryContext, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id); err != nil {
			return err
		} else {
			if row == nil {
				stats, err := api.ResultSetStats(queryContext, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
				if err != nil {
					return err
				}
				nextMetadata, nextResultSetErr := api.NextResultSet(queryContext, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
				res := &pb.RowData{Rows: rows, Stats: stats, HasMoreResults: nextMetadata != nil || nextResultSetErr != nil}
				if first {
					res.Metadata = metadata
					first = false
				}
				if err := stream.Send(res); err != nil {
					return err
				}
				if nextResultSetErr != nil {
					return nextResultSetErr
				}
				if res.HasMoreResults {
					metadata = nextMetadata
					first = true
					continue
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

func (s *spannerLibServer) ConnectionStream(stream grpc.BidiStreamingServer[pb.ConnectionStreamRequest, pb.ConnectionStreamResponse]) error {
	for {
		var err error
		var response *pb.ConnectionStreamResponse
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if req.GetExecuteRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleExecuteRequest(ctx, req.GetExecuteRequest())
		} else if req.GetExecuteBatchRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleExecuteBatchRequest(ctx, req.GetExecuteBatchRequest())
		} else if req.GetWriteMutationsRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleWriteMutationsRequest(ctx, req.GetWriteMutationsRequest())
		} else if req.GetBeginTransactionRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleBeginTransactionRequest(ctx, req.GetBeginTransactionRequest())
		} else if req.GetCommitRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleCommitRequest(ctx, req.GetCommitRequest())
		} else if req.GetRollbackRequest() != nil {
			ctx := stream.Context()
			response, err = s.handleRollbackRequest(ctx, req.GetRollbackRequest())
		} else {
			return gstatus.Errorf(codes.Unimplemented, "unsupported request type: %v", req.Request)
		}
		if err != nil {
			response = &pb.ConnectionStreamResponse{Status: gstatus.Convert(err).Proto()}
		}
		if stream.Send(response) != nil {
			return err
		}
	}
}

func (s *spannerLibServer) handleExecuteRequest(ctx context.Context, request *pb.ExecuteRequest) (*pb.ConnectionStreamResponse, error) {
	maxFetchRows := int64(50)
	if request.FetchOptions != nil && request.FetchOptions.NumRows > 0 {
		maxFetchRows = request.FetchOptions.NumRows
	}

	rows, err := s.Execute(ctx, request)
	if err != nil {
		return nil, err
	}
	response := &pb.ConnectionStreamResponse_ExecuteResponse{ExecuteResponse: &pb.ExecuteResponse{Rows: rows}}
	defer func() {
		if !response.ExecuteResponse.HasMoreResults {
			_ = api.CloseRows(context.Background(), rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
		}
	}()

	metadata, err := api.Metadata(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	numRows := int64(0)
	for {
		resultSet := &spannerpb.ResultSet{}
		if err != nil {
			return nil, err
		}
		resultSet.Metadata = metadata
		response.ExecuteResponse.ResultSets = append(response.ExecuteResponse.ResultSets, resultSet)
		for {
			row, err := api.Next(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
			if err != nil {
				if len(response.ExecuteResponse.ResultSets) == 1 {
					return nil, err
				}
				// Remove the last result set from the response and return an error code for it instead.
				response.ExecuteResponse.ResultSets = response.ExecuteResponse.ResultSets[:len(response.ExecuteResponse.ResultSets)-1]
				response.ExecuteResponse.Status = gstatus.Convert(err).Proto()
				return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: response}, nil
			}
			if row == nil {
				break
			}
			resultSet.Rows = append(resultSet.Rows, row)
			numRows++
			if numRows == maxFetchRows {
				response.ExecuteResponse.HasMoreResults = true
				return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: response}, nil
			}
		}

		stats, err := api.ResultSetStats(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
		if err != nil {
			if len(response.ExecuteResponse.ResultSets) == 1 {
				return nil, err
			}
			// Remove the last result set from the response and return an error code for it instead.
			response.ExecuteResponse.ResultSets = response.ExecuteResponse.ResultSets[:len(response.ExecuteResponse.ResultSets)-1]
			response.ExecuteResponse.Status = gstatus.Convert(err).Proto()
			return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: response}, nil
		}
		resultSet.Stats = stats

		metadata, err = api.NextResultSet(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
		if err != nil {
			response.ExecuteResponse.Status = gstatus.Convert(err).Proto()
			return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: response}, nil
		}
		if metadata == nil {
			break
		}
	}
	response.ExecuteResponse.Status = &status.Status{}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: response}, nil
}

func (s *spannerLibServer) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	resp, err := api.ExecuteBatch(ctx, request.Connection.Pool.Id, request.Connection.Id, request.ExecuteBatchDmlRequest)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) handleExecuteBatchRequest(ctx context.Context, request *pb.ExecuteBatchRequest) (*pb.ConnectionStreamResponse, error) {
	res, err := s.ExecuteBatch(ctx, request)
	if err != nil {
		return nil, err
	}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: &pb.ConnectionStreamResponse_ExecuteBatchResponse{ExecuteBatchResponse: res}}, nil
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
	values, err := api.NextBuffered(ctx, request.Rows.Connection.Pool.Id, request.Rows.Connection.Id, request.Rows.Id)
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

func (s *spannerLibServer) NextResultSet(ctx context.Context, rows *pb.Rows) (*spannerpb.ResultSetMetadata, error) {
	metadata, err := api.NextResultSet(ctx, rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return metadata, nil
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
	txContext := context.WithoutCancel(ctx)
	err := api.BeginTransaction(txContext, request.Connection.Pool.Id, request.Connection.Id, request.TransactionOptions)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) handleBeginTransactionRequest(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.ConnectionStreamResponse, error) {
	res, err := s.BeginTransaction(ctx, request)
	if err != nil {
		return nil, err
	}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: &pb.ConnectionStreamResponse_BeginTransactionResponse{BeginTransactionResponse: res}}, nil
}

func (s *spannerLibServer) Commit(ctx context.Context, connection *pb.Connection) (*spannerpb.CommitResponse, error) {
	resp, err := api.Commit(ctx, connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) handleCommitRequest(ctx context.Context, connection *pb.Connection) (*pb.ConnectionStreamResponse, error) {
	res, err := s.Commit(ctx, connection)
	if err != nil {
		return nil, err
	}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: &pb.ConnectionStreamResponse_CommitResponse{CommitResponse: res}}, nil
}

func (s *spannerLibServer) Rollback(ctx context.Context, connection *pb.Connection) (*emptypb.Empty, error) {
	err := api.Rollback(ctx, connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) handleRollbackRequest(ctx context.Context, connection *pb.Connection) (*pb.ConnectionStreamResponse, error) {
	res, err := s.Rollback(ctx, connection)
	if err != nil {
		return nil, err
	}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: &pb.ConnectionStreamResponse_RollbackResponse{RollbackResponse: res}}, nil
}

func (s *spannerLibServer) WriteMutations(ctx context.Context, request *pb.WriteMutationsRequest) (*spannerpb.CommitResponse, error) {
	resp, err := api.WriteMutations(ctx, request.Connection.Pool.Id, request.Connection.Id, request.Mutations)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) handleWriteMutationsRequest(ctx context.Context, request *pb.WriteMutationsRequest) (*pb.ConnectionStreamResponse, error) {
	res, err := s.WriteMutations(ctx, request)
	if err != nil {
		return nil, err
	}
	return &pb.ConnectionStreamResponse{Status: &status.Status{}, Response: &pb.ConnectionStreamResponse_WriteMutationsResponse{WriteMutationsResponse: res}}, nil
}
