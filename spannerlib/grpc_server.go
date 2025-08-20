package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"spannerlib/api"
	pb "spannerlib/google/spannerlib/v1"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Missing gRPC server address\n")
	}
	name := os.Args[1]
	defer func() { _ = os.Remove(name) }()
	lis, err := net.Listen("unix", name)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	server := spannerLibServer{}
	pb.RegisterSpannerLibServer(grpcServer, &server)
	log.Printf("Starting gRPC server on %s\n", name)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v\n", err)
	} else {
		log.Printf("gRPC server started\n")
	}
}

var _ pb.SpannerLibServer = &spannerLibServer{}

type spannerLibServer struct {
	pb.UnimplementedSpannerLibServer
}

func (s *spannerLibServer) CreatePool(ctx context.Context, request *pb.CreatePoolRequest) (*pb.Pool, error) {
	id, err := api.CreatePool(request.Dsn)
	if err != nil {
		return nil, err
	}
	return &pb.Pool{Id: id}, nil
}

func (s *spannerLibServer) ClosePool(ctx context.Context, pool *pb.Pool) (*emptypb.Empty, error) {
	err := api.ClosePool(pool.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) CreateConnection(ctx context.Context, request *pb.CreateConnectionRequest) (*pb.Connection, error) {
	id, err := api.CreateConnection(request.Pool.Id)
	if err != nil {
		return nil, err
	}
	return &pb.Connection{Pool: request.Pool, Id: id}, nil
}

func (s *spannerLibServer) CloseConnection(ctx context.Context, connection *pb.Connection) (*emptypb.Empty, error) {
	err := api.CloseConnection(connection.Pool.Id, connection.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) Execute(ctx context.Context, request *pb.ExecuteRequest) (*pb.Rows, error) {
	id, err := api.Execute(request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	if err != nil {
		return nil, err
	}
	return &pb.Rows{Connection: request.Connection, Id: id}, nil
}

func (s *spannerLibServer) ExecuteStreaming(request *pb.ExecuteRequest, stream grpc.ServerStreamingServer[spannerpb.PartialResultSet]) error {
	return s.executeStreaming(request, stream)
}

type partialResultSetStream interface {
	Send(*spannerpb.PartialResultSet) error
}

func (s *spannerLibServer) executeStreaming(request *pb.ExecuteRequest, stream partialResultSetStream) error {
	id, err := api.Execute(request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
	defer func() { _ = api.CloseRows(request.Connection.Pool.Id, request.Connection.Id, id) }()
	if err != nil {
		return err
	}
	metadata, err := api.Metadata(request.Connection.Pool.Id, request.Connection.Id, id)
	first := true
	for {
		if row, err := api.Next(request.Connection.Pool.Id, request.Connection.Id, id); err != nil {
			return err
		} else {
			if row == nil {
				stats, err := api.ResultSetStats(request.Connection.Pool.Id, request.Connection.Id, id)
				if err != nil {
					return err
				}
				res := &spannerpb.PartialResultSet{Stats: stats, Last: true}
				if err := stream.Send(res); err != nil {
					return err
				}
				break
			}
			res := &spannerpb.PartialResultSet{Values: row.Values}
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

func (s *spannerLibServer) sendData(data chan *spannerpb.PartialResultSet, stream partialResultSetStream) error {
	for {
		res, ok := <-data
		if !ok {
			return nil
		}
		if err := stream.Send(res); err != nil {
			close(data)
			return err
		}
	}
}

//func (s *spannerLibServer) produceData(data chan *spannerpb.PartialResultSet, request *pb.ExecuteRequest) {
//	id, err := api.Execute(request.Connection.Pool.Id, request.Connection.Id, request.ExecuteSqlRequest)
//	defer func() { _ = api.CloseRows(request.Connection.Pool.Id, request.Connection.Id, id) }()
//	if err != nil {
//		return err
//	}
//	metadata, err := api.Metadata(request.Connection.Pool.Id, request.Connection.Id, id)
//	first := true
//	for {
//		if row, err := api.Next(request.Connection.Pool.Id, request.Connection.Id, id); err != nil {
//			return err
//		} else {
//			if row == nil {
//				stats, err := api.ResultSetStats(request.Connection.Pool.Id, request.Connection.Id, id)
//				if err != nil {
//					return err
//				}
//				res := &spannerpb.PartialResultSet{Stats: stats, Last: true}
//				if err := stream.Send(res); err != nil {
//					return err
//				}
//				break
//			}
//			res := &spannerpb.PartialResultSet{Values: row.Values}
//			if first {
//				res.Metadata = metadata
//				first = false
//			}
//			if err := stream.Send(res); err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

func (s *spannerLibServer) ExecuteTransaction(ctx context.Context, request *pb.ExecuteTransactionRequest) (*pb.Rows, error) {
	id, err := api.ExecuteTransaction(request.Transaction.Connection.Pool.Id, request.Transaction.Connection.Id, request.Transaction.Id, request.ExecuteSqlRequest)
	if err != nil {
		return nil, err
	}
	return &pb.Rows{Connection: request.Transaction.Connection, Id: id}, nil
}
func (s *spannerLibServer) ExecuteBatchDml(ctx context.Context, request *pb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	resp, err := api.ExecuteBatchDml(request.Connection.Pool.Id, request.Connection.Id, request.ExecuteBatchDmlRequest)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) Metadata(ctx context.Context, rows *pb.Rows) (*spannerpb.ResultSetMetadata, error) {
	metadata, err := api.Metadata(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *spannerLibServer) Next(ctx context.Context, rows *pb.Rows) (*structpb.ListValue, error) {
	values, err := api.Next(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (s *spannerLibServer) ResultSetStats(ctx context.Context, rows *pb.Rows) (*spannerpb.ResultSetStats, error) {
	stats, err := api.ResultSetStats(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *spannerLibServer) CloseRows(ctx context.Context, rows *pb.Rows) (*emptypb.Empty, error) {
	err := api.CloseRows(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.Transaction, error) {
	id, err := api.BeginTransaction(request.Connection.Pool.Id, request.Connection.Id, request.TransactionOptions)
	if err != nil {
		return nil, err
	}
	return &pb.Transaction{Connection: request.Connection, Id: id}, nil
}

func (s *spannerLibServer) Commit(ctx context.Context, request *pb.Transaction) (*spannerpb.CommitResponse, error) {
	resp, err := api.Commit(request.Connection.Pool.Id, request.Connection.Id, request.Id)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) Rollback(ctx context.Context, request *pb.Transaction) (*emptypb.Empty, error) {
	err := api.Rollback(request.Connection.Pool.Id, request.Connection.Id, request.Id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) Apply(ctx context.Context, request *pb.ApplyRequest) (*spannerpb.CommitResponse, error) {
	resp, err := api.Apply(request.Connection.Pool.Id, request.Connection.Id, request.Mutations)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *spannerLibServer) BufferWrite(ctx context.Context, request *pb.BufferWriteRequest) (*emptypb.Empty, error) {
	err := api.BufferWrite(request.Transaction.Connection.Pool.Id, request.Transaction.Connection.Id, request.Transaction.Id, request.Mutations)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *spannerLibServer) ConnectionStream(stream grpc.BidiStreamingServer[pb.ConnectionStreamRequest, pb.ConnectionStreamResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetExecuteRequest() != nil {
			if err := s.handleExecuteRequest(stream, req.GetExecuteRequest()); err != nil {
				return err
			}
		}
	}
}

type connectionStreamResponseStream struct {
	stream grpc.BidiStreamingServer[pb.ConnectionStreamRequest, pb.ConnectionStreamResponse]
}

func (cs *connectionStreamResponseStream) Send(row *spannerpb.PartialResultSet) error {
	return cs.stream.Send(&pb.ConnectionStreamResponse{Response: &pb.ConnectionStreamResponse_Row{Row: row}})
}

func (s *spannerLibServer) handleExecuteRequest(stream grpc.BidiStreamingServer[pb.ConnectionStreamRequest, pb.ConnectionStreamResponse], req *pb.ExecuteRequest) error {
	return s.executeStreaming(req, &connectionStreamResponseStream{stream: stream})
}
