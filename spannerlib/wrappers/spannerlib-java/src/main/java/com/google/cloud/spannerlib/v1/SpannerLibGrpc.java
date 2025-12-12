package com.google.cloud.spannerlib.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class SpannerLibGrpc {

  private SpannerLibGrpc() {}

  public static final java.lang.String SERVICE_NAME = "google.spannerlib.v1.SpannerLib";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.InfoRequest,
      com.google.cloud.spannerlib.v1.InfoResponse> getInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Info",
      requestType = com.google.cloud.spannerlib.v1.InfoRequest.class,
      responseType = com.google.cloud.spannerlib.v1.InfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.InfoRequest,
      com.google.cloud.spannerlib.v1.InfoResponse> getInfoMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.InfoRequest, com.google.cloud.spannerlib.v1.InfoResponse> getInfoMethod;
    if ((getInfoMethod = SpannerLibGrpc.getInfoMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getInfoMethod = SpannerLibGrpc.getInfoMethod) == null) {
          SpannerLibGrpc.getInfoMethod = getInfoMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.InfoRequest, com.google.cloud.spannerlib.v1.InfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Info"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.InfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.InfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Info"))
              .build();
        }
      }
    }
    return getInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreatePoolRequest,
      com.google.cloud.spannerlib.v1.Pool> getCreatePoolMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreatePool",
      requestType = com.google.cloud.spannerlib.v1.CreatePoolRequest.class,
      responseType = com.google.cloud.spannerlib.v1.Pool.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreatePoolRequest,
      com.google.cloud.spannerlib.v1.Pool> getCreatePoolMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreatePoolRequest, com.google.cloud.spannerlib.v1.Pool> getCreatePoolMethod;
    if ((getCreatePoolMethod = SpannerLibGrpc.getCreatePoolMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getCreatePoolMethod = SpannerLibGrpc.getCreatePoolMethod) == null) {
          SpannerLibGrpc.getCreatePoolMethod = getCreatePoolMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.CreatePoolRequest, com.google.cloud.spannerlib.v1.Pool>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreatePool"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.CreatePoolRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Pool.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("CreatePool"))
              .build();
        }
      }
    }
    return getCreatePoolMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Pool,
      com.google.protobuf.Empty> getClosePoolMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ClosePool",
      requestType = com.google.cloud.spannerlib.v1.Pool.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Pool,
      com.google.protobuf.Empty> getClosePoolMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Pool, com.google.protobuf.Empty> getClosePoolMethod;
    if ((getClosePoolMethod = SpannerLibGrpc.getClosePoolMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getClosePoolMethod = SpannerLibGrpc.getClosePoolMethod) == null) {
          SpannerLibGrpc.getClosePoolMethod = getClosePoolMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Pool, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClosePool"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Pool.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ClosePool"))
              .build();
        }
      }
    }
    return getClosePoolMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreateConnectionRequest,
      com.google.cloud.spannerlib.v1.Connection> getCreateConnectionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateConnection",
      requestType = com.google.cloud.spannerlib.v1.CreateConnectionRequest.class,
      responseType = com.google.cloud.spannerlib.v1.Connection.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreateConnectionRequest,
      com.google.cloud.spannerlib.v1.Connection> getCreateConnectionMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.CreateConnectionRequest, com.google.cloud.spannerlib.v1.Connection> getCreateConnectionMethod;
    if ((getCreateConnectionMethod = SpannerLibGrpc.getCreateConnectionMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getCreateConnectionMethod = SpannerLibGrpc.getCreateConnectionMethod) == null) {
          SpannerLibGrpc.getCreateConnectionMethod = getCreateConnectionMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.CreateConnectionRequest, com.google.cloud.spannerlib.v1.Connection>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateConnection"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.CreateConnectionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Connection.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("CreateConnection"))
              .build();
        }
      }
    }
    return getCreateConnectionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.protobuf.Empty> getCloseConnectionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseConnection",
      requestType = com.google.cloud.spannerlib.v1.Connection.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.protobuf.Empty> getCloseConnectionMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection, com.google.protobuf.Empty> getCloseConnectionMethod;
    if ((getCloseConnectionMethod = SpannerLibGrpc.getCloseConnectionMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getCloseConnectionMethod = SpannerLibGrpc.getCloseConnectionMethod) == null) {
          SpannerLibGrpc.getCloseConnectionMethod = getCloseConnectionMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Connection, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseConnection"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Connection.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("CloseConnection"))
              .build();
        }
      }
    }
    return getCloseConnectionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest,
      com.google.cloud.spannerlib.v1.Rows> getExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Execute",
      requestType = com.google.cloud.spannerlib.v1.ExecuteRequest.class,
      responseType = com.google.cloud.spannerlib.v1.Rows.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest,
      com.google.cloud.spannerlib.v1.Rows> getExecuteMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest, com.google.cloud.spannerlib.v1.Rows> getExecuteMethod;
    if ((getExecuteMethod = SpannerLibGrpc.getExecuteMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getExecuteMethod = SpannerLibGrpc.getExecuteMethod) == null) {
          SpannerLibGrpc.getExecuteMethod = getExecuteMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.ExecuteRequest, com.google.cloud.spannerlib.v1.Rows>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Execute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.ExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Execute"))
              .build();
        }
      }
    }
    return getExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest,
      com.google.cloud.spannerlib.v1.RowData> getExecuteStreamingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExecuteStreaming",
      requestType = com.google.cloud.spannerlib.v1.ExecuteRequest.class,
      responseType = com.google.cloud.spannerlib.v1.RowData.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest,
      com.google.cloud.spannerlib.v1.RowData> getExecuteStreamingMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteRequest, com.google.cloud.spannerlib.v1.RowData> getExecuteStreamingMethod;
    if ((getExecuteStreamingMethod = SpannerLibGrpc.getExecuteStreamingMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getExecuteStreamingMethod = SpannerLibGrpc.getExecuteStreamingMethod) == null) {
          SpannerLibGrpc.getExecuteStreamingMethod = getExecuteStreamingMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.ExecuteRequest, com.google.cloud.spannerlib.v1.RowData>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExecuteStreaming"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.ExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.RowData.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ExecuteStreaming"))
              .build();
        }
      }
    }
    return getExecuteStreamingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteBatchRequest,
      com.google.spanner.v1.ExecuteBatchDmlResponse> getExecuteBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExecuteBatch",
      requestType = com.google.cloud.spannerlib.v1.ExecuteBatchRequest.class,
      responseType = com.google.spanner.v1.ExecuteBatchDmlResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteBatchRequest,
      com.google.spanner.v1.ExecuteBatchDmlResponse> getExecuteBatchMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ExecuteBatchRequest, com.google.spanner.v1.ExecuteBatchDmlResponse> getExecuteBatchMethod;
    if ((getExecuteBatchMethod = SpannerLibGrpc.getExecuteBatchMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getExecuteBatchMethod = SpannerLibGrpc.getExecuteBatchMethod) == null) {
          SpannerLibGrpc.getExecuteBatchMethod = getExecuteBatchMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.ExecuteBatchRequest, com.google.spanner.v1.ExecuteBatchDmlResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExecuteBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.ExecuteBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.ExecuteBatchDmlResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ExecuteBatch"))
              .build();
        }
      }
    }
    return getExecuteBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetMetadata> getMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Metadata",
      requestType = com.google.cloud.spannerlib.v1.Rows.class,
      responseType = com.google.spanner.v1.ResultSetMetadata.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetMetadata> getMetadataMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetMetadata> getMetadataMethod;
    if ((getMetadataMethod = SpannerLibGrpc.getMetadataMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getMetadataMethod = SpannerLibGrpc.getMetadataMethod) == null) {
          SpannerLibGrpc.getMetadataMethod = getMetadataMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetMetadata>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Metadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.ResultSetMetadata.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Metadata"))
              .build();
        }
      }
    }
    return getMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.NextRequest,
      com.google.protobuf.ListValue> getNextMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Next",
      requestType = com.google.cloud.spannerlib.v1.NextRequest.class,
      responseType = com.google.protobuf.ListValue.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.NextRequest,
      com.google.protobuf.ListValue> getNextMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.NextRequest, com.google.protobuf.ListValue> getNextMethod;
    if ((getNextMethod = SpannerLibGrpc.getNextMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getNextMethod = SpannerLibGrpc.getNextMethod) == null) {
          SpannerLibGrpc.getNextMethod = getNextMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.NextRequest, com.google.protobuf.ListValue>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Next"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.NextRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.ListValue.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Next"))
              .build();
        }
      }
    }
    return getNextMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetStats> getResultSetStatsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResultSetStats",
      requestType = com.google.cloud.spannerlib.v1.Rows.class,
      responseType = com.google.spanner.v1.ResultSetStats.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetStats> getResultSetStatsMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetStats> getResultSetStatsMethod;
    if ((getResultSetStatsMethod = SpannerLibGrpc.getResultSetStatsMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getResultSetStatsMethod = SpannerLibGrpc.getResultSetStatsMethod) == null) {
          SpannerLibGrpc.getResultSetStatsMethod = getResultSetStatsMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetStats>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResultSetStats"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.ResultSetStats.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ResultSetStats"))
              .build();
        }
      }
    }
    return getResultSetStatsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetMetadata> getNextResultSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NextResultSet",
      requestType = com.google.cloud.spannerlib.v1.Rows.class,
      responseType = com.google.spanner.v1.ResultSetMetadata.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.spanner.v1.ResultSetMetadata> getNextResultSetMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetMetadata> getNextResultSetMethod;
    if ((getNextResultSetMethod = SpannerLibGrpc.getNextResultSetMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getNextResultSetMethod = SpannerLibGrpc.getNextResultSetMethod) == null) {
          SpannerLibGrpc.getNextResultSetMethod = getNextResultSetMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Rows, com.google.spanner.v1.ResultSetMetadata>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NextResultSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.ResultSetMetadata.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("NextResultSet"))
              .build();
        }
      }
    }
    return getNextResultSetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.protobuf.Empty> getCloseRowsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseRows",
      requestType = com.google.cloud.spannerlib.v1.Rows.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.protobuf.Empty> getCloseRowsMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows, com.google.protobuf.Empty> getCloseRowsMethod;
    if ((getCloseRowsMethod = SpannerLibGrpc.getCloseRowsMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getCloseRowsMethod = SpannerLibGrpc.getCloseRowsMethod) == null) {
          SpannerLibGrpc.getCloseRowsMethod = getCloseRowsMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Rows, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseRows"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("CloseRows"))
              .build();
        }
      }
    }
    return getCloseRowsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.BeginTransactionRequest,
      com.google.protobuf.Empty> getBeginTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BeginTransaction",
      requestType = com.google.cloud.spannerlib.v1.BeginTransactionRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.BeginTransactionRequest,
      com.google.protobuf.Empty> getBeginTransactionMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.BeginTransactionRequest, com.google.protobuf.Empty> getBeginTransactionMethod;
    if ((getBeginTransactionMethod = SpannerLibGrpc.getBeginTransactionMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getBeginTransactionMethod = SpannerLibGrpc.getBeginTransactionMethod) == null) {
          SpannerLibGrpc.getBeginTransactionMethod = getBeginTransactionMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.BeginTransactionRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BeginTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.BeginTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("BeginTransaction"))
              .build();
        }
      }
    }
    return getBeginTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.spanner.v1.CommitResponse> getCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Commit",
      requestType = com.google.cloud.spannerlib.v1.Connection.class,
      responseType = com.google.spanner.v1.CommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.spanner.v1.CommitResponse> getCommitMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection, com.google.spanner.v1.CommitResponse> getCommitMethod;
    if ((getCommitMethod = SpannerLibGrpc.getCommitMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getCommitMethod = SpannerLibGrpc.getCommitMethod) == null) {
          SpannerLibGrpc.getCommitMethod = getCommitMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Connection, com.google.spanner.v1.CommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Connection.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.CommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Commit"))
              .build();
        }
      }
    }
    return getCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.protobuf.Empty> getRollbackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rollback",
      requestType = com.google.cloud.spannerlib.v1.Connection.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection,
      com.google.protobuf.Empty> getRollbackMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Connection, com.google.protobuf.Empty> getRollbackMethod;
    if ((getRollbackMethod = SpannerLibGrpc.getRollbackMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getRollbackMethod = SpannerLibGrpc.getRollbackMethod) == null) {
          SpannerLibGrpc.getRollbackMethod = getRollbackMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Connection, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rollback"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Connection.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("Rollback"))
              .build();
        }
      }
    }
    return getRollbackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.WriteMutationsRequest,
      com.google.spanner.v1.CommitResponse> getWriteMutationsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteMutations",
      requestType = com.google.cloud.spannerlib.v1.WriteMutationsRequest.class,
      responseType = com.google.spanner.v1.CommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.WriteMutationsRequest,
      com.google.spanner.v1.CommitResponse> getWriteMutationsMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.WriteMutationsRequest, com.google.spanner.v1.CommitResponse> getWriteMutationsMethod;
    if ((getWriteMutationsMethod = SpannerLibGrpc.getWriteMutationsMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getWriteMutationsMethod = SpannerLibGrpc.getWriteMutationsMethod) == null) {
          SpannerLibGrpc.getWriteMutationsMethod = getWriteMutationsMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.WriteMutationsRequest, com.google.spanner.v1.CommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteMutations"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.WriteMutationsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.spanner.v1.CommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("WriteMutations"))
              .build();
        }
      }
    }
    return getWriteMutationsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ConnectionStreamRequest,
      com.google.cloud.spannerlib.v1.ConnectionStreamResponse> getConnectionStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ConnectionStream",
      requestType = com.google.cloud.spannerlib.v1.ConnectionStreamRequest.class,
      responseType = com.google.cloud.spannerlib.v1.ConnectionStreamResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ConnectionStreamRequest,
      com.google.cloud.spannerlib.v1.ConnectionStreamResponse> getConnectionStreamMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.ConnectionStreamRequest, com.google.cloud.spannerlib.v1.ConnectionStreamResponse> getConnectionStreamMethod;
    if ((getConnectionStreamMethod = SpannerLibGrpc.getConnectionStreamMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getConnectionStreamMethod = SpannerLibGrpc.getConnectionStreamMethod) == null) {
          SpannerLibGrpc.getConnectionStreamMethod = getConnectionStreamMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.ConnectionStreamRequest, com.google.cloud.spannerlib.v1.ConnectionStreamResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ConnectionStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.ConnectionStreamRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.ConnectionStreamResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ConnectionStream"))
              .build();
        }
      }
    }
    return getConnectionStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.cloud.spannerlib.v1.RowData> getContinueStreamingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ContinueStreaming",
      requestType = com.google.cloud.spannerlib.v1.Rows.class,
      responseType = com.google.cloud.spannerlib.v1.RowData.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows,
      com.google.cloud.spannerlib.v1.RowData> getContinueStreamingMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.spannerlib.v1.Rows, com.google.cloud.spannerlib.v1.RowData> getContinueStreamingMethod;
    if ((getContinueStreamingMethod = SpannerLibGrpc.getContinueStreamingMethod) == null) {
      synchronized (SpannerLibGrpc.class) {
        if ((getContinueStreamingMethod = SpannerLibGrpc.getContinueStreamingMethod) == null) {
          SpannerLibGrpc.getContinueStreamingMethod = getContinueStreamingMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.spannerlib.v1.Rows, com.google.cloud.spannerlib.v1.RowData>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ContinueStreaming"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.Rows.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.spannerlib.v1.RowData.getDefaultInstance()))
              .setSchemaDescriptor(new SpannerLibMethodDescriptorSupplier("ContinueStreaming"))
              .build();
        }
      }
    }
    return getContinueStreamingMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SpannerLibStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SpannerLibStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SpannerLibStub>() {
        @java.lang.Override
        public SpannerLibStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SpannerLibStub(channel, callOptions);
        }
      };
    return SpannerLibStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static SpannerLibBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SpannerLibBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SpannerLibBlockingV2Stub>() {
        @java.lang.Override
        public SpannerLibBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SpannerLibBlockingV2Stub(channel, callOptions);
        }
      };
    return SpannerLibBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SpannerLibBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SpannerLibBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SpannerLibBlockingStub>() {
        @java.lang.Override
        public SpannerLibBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SpannerLibBlockingStub(channel, callOptions);
        }
      };
    return SpannerLibBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SpannerLibFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SpannerLibFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SpannerLibFutureStub>() {
        @java.lang.Override
        public SpannerLibFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SpannerLibFutureStub(channel, callOptions);
        }
      };
    return SpannerLibFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void info(com.google.cloud.spannerlib.v1.InfoRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.InfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInfoMethod(), responseObserver);
    }

    /**
     */
    default void createPool(com.google.cloud.spannerlib.v1.CreatePoolRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Pool> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreatePoolMethod(), responseObserver);
    }

    /**
     */
    default void closePool(com.google.cloud.spannerlib.v1.Pool request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClosePoolMethod(), responseObserver);
    }

    /**
     */
    default void createConnection(com.google.cloud.spannerlib.v1.CreateConnectionRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Connection> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateConnectionMethod(), responseObserver);
    }

    /**
     */
    default void closeConnection(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseConnectionMethod(), responseObserver);
    }

    /**
     */
    default void execute(com.google.cloud.spannerlib.v1.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Rows> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteMethod(), responseObserver);
    }

    /**
     */
    default void executeStreaming(com.google.cloud.spannerlib.v1.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteStreamingMethod(), responseObserver);
    }

    /**
     */
    default void executeBatch(com.google.cloud.spannerlib.v1.ExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ExecuteBatchDmlResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteBatchMethod(), responseObserver);
    }

    /**
     */
    default void metadata(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMetadataMethod(), responseObserver);
    }

    /**
     */
    default void next(com.google.cloud.spannerlib.v1.NextRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.ListValue> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNextMethod(), responseObserver);
    }

    /**
     */
    default void resultSetStats(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetStats> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResultSetStatsMethod(), responseObserver);
    }

    /**
     */
    default void nextResultSet(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNextResultSetMethod(), responseObserver);
    }

    /**
     */
    default void closeRows(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseRowsMethod(), responseObserver);
    }

    /**
     */
    default void beginTransaction(com.google.cloud.spannerlib.v1.BeginTransactionRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBeginTransactionMethod(), responseObserver);
    }

    /**
     */
    default void commit(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
    }

    /**
     */
    default void rollback(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackMethod(), responseObserver);
    }

    /**
     */
    default void writeMutations(com.google.cloud.spannerlib.v1.WriteMutationsRequest request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMutationsMethod(), responseObserver);
    }

    /**
     * <pre>
     * ConnectionStream opens a bi-directional gRPC stream between the client and the server.
     * This stream can be re-used by the client for multiple requests, and normally gives the
     * lowest possible latency, at the cost of a slightly more complex API.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.ConnectionStreamRequest> connectionStream(
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.ConnectionStreamResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getConnectionStreamMethod(), responseObserver);
    }

    /**
     * <pre>
     * ContinueStreaming returns a server stream that returns the remaining rows of a SQL statement
     * that has previously been executed using a ConnectionStreamRequest on a bi-directional
     * ConnectionStream. The client is responsible for calling this RPC if the has_more_data flag
     * of the ExecuteResponse was true.
     * </pre>
     */
    default void continueStreaming(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getContinueStreamingMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service SpannerLib.
   */
  public static abstract class SpannerLibImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return SpannerLibGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service SpannerLib.
   */
  public static final class SpannerLibStub
      extends io.grpc.stub.AbstractAsyncStub<SpannerLibStub> {
    private SpannerLibStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SpannerLibStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SpannerLibStub(channel, callOptions);
    }

    /**
     */
    public void info(com.google.cloud.spannerlib.v1.InfoRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.InfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createPool(com.google.cloud.spannerlib.v1.CreatePoolRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Pool> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreatePoolMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void closePool(com.google.cloud.spannerlib.v1.Pool request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getClosePoolMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createConnection(com.google.cloud.spannerlib.v1.CreateConnectionRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Connection> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateConnectionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void closeConnection(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseConnectionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void execute(com.google.cloud.spannerlib.v1.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Rows> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void executeStreaming(com.google.cloud.spannerlib.v1.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getExecuteStreamingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void executeBatch(com.google.cloud.spannerlib.v1.ExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ExecuteBatchDmlResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExecuteBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void metadata(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void next(com.google.cloud.spannerlib.v1.NextRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.ListValue> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNextMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void resultSetStats(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetStats> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResultSetStatsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void nextResultSet(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNextResultSetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void closeRows(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseRowsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void beginTransaction(com.google.cloud.spannerlib.v1.BeginTransactionRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBeginTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commit(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rollback(com.google.cloud.spannerlib.v1.Connection request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeMutations(com.google.cloud.spannerlib.v1.WriteMutationsRequest request,
        io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMutationsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ConnectionStream opens a bi-directional gRPC stream between the client and the server.
     * This stream can be re-used by the client for multiple requests, and normally gives the
     * lowest possible latency, at the cost of a slightly more complex API.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.ConnectionStreamRequest> connectionStream(
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.ConnectionStreamResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getConnectionStreamMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * ContinueStreaming returns a server stream that returns the remaining rows of a SQL statement
     * that has previously been executed using a ConnectionStreamRequest on a bi-directional
     * ConnectionStream. The client is responsible for calling this RPC if the has_more_data flag
     * of the ExecuteResponse was true.
     * </pre>
     */
    public void continueStreaming(com.google.cloud.spannerlib.v1.Rows request,
        io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getContinueStreamingMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service SpannerLib.
   */
  public static final class SpannerLibBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<SpannerLibBlockingV2Stub> {
    private SpannerLibBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SpannerLibBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SpannerLibBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.InfoResponse info(com.google.cloud.spannerlib.v1.InfoRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Pool createPool(com.google.cloud.spannerlib.v1.CreatePoolRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreatePoolMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closePool(com.google.cloud.spannerlib.v1.Pool request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getClosePoolMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Connection createConnection(com.google.cloud.spannerlib.v1.CreateConnectionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateConnectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closeConnection(com.google.cloud.spannerlib.v1.Connection request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCloseConnectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Rows execute(com.google.cloud.spannerlib.v1.ExecuteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getExecuteMethod(), getCallOptions(), request);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.google.cloud.spannerlib.v1.RowData>
        executeStreaming(com.google.cloud.spannerlib.v1.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getExecuteStreamingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ExecuteBatchDmlResponse executeBatch(com.google.cloud.spannerlib.v1.ExecuteBatchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getExecuteBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetMetadata metadata(com.google.cloud.spannerlib.v1.Rows request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.ListValue next(com.google.cloud.spannerlib.v1.NextRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getNextMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetStats resultSetStats(com.google.cloud.spannerlib.v1.Rows request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResultSetStatsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetMetadata nextResultSet(com.google.cloud.spannerlib.v1.Rows request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getNextResultSetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closeRows(com.google.cloud.spannerlib.v1.Rows request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCloseRowsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty beginTransaction(com.google.cloud.spannerlib.v1.BeginTransactionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBeginTransactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.CommitResponse commit(com.google.cloud.spannerlib.v1.Connection request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty rollback(com.google.cloud.spannerlib.v1.Connection request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRollbackMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.CommitResponse writeMutations(com.google.cloud.spannerlib.v1.WriteMutationsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getWriteMutationsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ConnectionStream opens a bi-directional gRPC stream between the client and the server.
     * This stream can be re-used by the client for multiple requests, and normally gives the
     * lowest possible latency, at the cost of a slightly more complex API.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.google.cloud.spannerlib.v1.ConnectionStreamRequest, com.google.cloud.spannerlib.v1.ConnectionStreamResponse>
        connectionStream() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getConnectionStreamMethod(), getCallOptions());
    }

    /**
     * <pre>
     * ContinueStreaming returns a server stream that returns the remaining rows of a SQL statement
     * that has previously been executed using a ConnectionStreamRequest on a bi-directional
     * ConnectionStream. The client is responsible for calling this RPC if the has_more_data flag
     * of the ExecuteResponse was true.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.google.cloud.spannerlib.v1.RowData>
        continueStreaming(com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getContinueStreamingMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service SpannerLib.
   */
  public static final class SpannerLibBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<SpannerLibBlockingStub> {
    private SpannerLibBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SpannerLibBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SpannerLibBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.InfoResponse info(com.google.cloud.spannerlib.v1.InfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Pool createPool(com.google.cloud.spannerlib.v1.CreatePoolRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreatePoolMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closePool(com.google.cloud.spannerlib.v1.Pool request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getClosePoolMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Connection createConnection(com.google.cloud.spannerlib.v1.CreateConnectionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateConnectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closeConnection(com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseConnectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.cloud.spannerlib.v1.Rows execute(com.google.cloud.spannerlib.v1.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExecuteMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.google.cloud.spannerlib.v1.RowData> executeStreaming(
        com.google.cloud.spannerlib.v1.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getExecuteStreamingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ExecuteBatchDmlResponse executeBatch(com.google.cloud.spannerlib.v1.ExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExecuteBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetMetadata metadata(com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.ListValue next(com.google.cloud.spannerlib.v1.NextRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNextMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetStats resultSetStats(com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResultSetStatsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.ResultSetMetadata nextResultSet(com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNextResultSetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closeRows(com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseRowsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty beginTransaction(com.google.cloud.spannerlib.v1.BeginTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBeginTransactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.CommitResponse commit(com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty rollback(com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.spanner.v1.CommitResponse writeMutations(com.google.cloud.spannerlib.v1.WriteMutationsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMutationsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ContinueStreaming returns a server stream that returns the remaining rows of a SQL statement
     * that has previously been executed using a ConnectionStreamRequest on a bi-directional
     * ConnectionStream. The client is responsible for calling this RPC if the has_more_data flag
     * of the ExecuteResponse was true.
     * </pre>
     */
    public java.util.Iterator<com.google.cloud.spannerlib.v1.RowData> continueStreaming(
        com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getContinueStreamingMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service SpannerLib.
   */
  public static final class SpannerLibFutureStub
      extends io.grpc.stub.AbstractFutureStub<SpannerLibFutureStub> {
    private SpannerLibFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SpannerLibFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SpannerLibFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.cloud.spannerlib.v1.InfoResponse> info(
        com.google.cloud.spannerlib.v1.InfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.cloud.spannerlib.v1.Pool> createPool(
        com.google.cloud.spannerlib.v1.CreatePoolRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreatePoolMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> closePool(
        com.google.cloud.spannerlib.v1.Pool request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getClosePoolMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.cloud.spannerlib.v1.Connection> createConnection(
        com.google.cloud.spannerlib.v1.CreateConnectionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateConnectionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> closeConnection(
        com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseConnectionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.cloud.spannerlib.v1.Rows> execute(
        com.google.cloud.spannerlib.v1.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExecuteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.ExecuteBatchDmlResponse> executeBatch(
        com.google.cloud.spannerlib.v1.ExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExecuteBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.ResultSetMetadata> metadata(
        com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMetadataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.ListValue> next(
        com.google.cloud.spannerlib.v1.NextRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNextMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.ResultSetStats> resultSetStats(
        com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResultSetStatsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.ResultSetMetadata> nextResultSet(
        com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNextResultSetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> closeRows(
        com.google.cloud.spannerlib.v1.Rows request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseRowsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> beginTransaction(
        com.google.cloud.spannerlib.v1.BeginTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBeginTransactionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.CommitResponse> commit(
        com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> rollback(
        com.google.cloud.spannerlib.v1.Connection request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.spanner.v1.CommitResponse> writeMutations(
        com.google.cloud.spannerlib.v1.WriteMutationsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMutationsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INFO = 0;
  private static final int METHODID_CREATE_POOL = 1;
  private static final int METHODID_CLOSE_POOL = 2;
  private static final int METHODID_CREATE_CONNECTION = 3;
  private static final int METHODID_CLOSE_CONNECTION = 4;
  private static final int METHODID_EXECUTE = 5;
  private static final int METHODID_EXECUTE_STREAMING = 6;
  private static final int METHODID_EXECUTE_BATCH = 7;
  private static final int METHODID_METADATA = 8;
  private static final int METHODID_NEXT = 9;
  private static final int METHODID_RESULT_SET_STATS = 10;
  private static final int METHODID_NEXT_RESULT_SET = 11;
  private static final int METHODID_CLOSE_ROWS = 12;
  private static final int METHODID_BEGIN_TRANSACTION = 13;
  private static final int METHODID_COMMIT = 14;
  private static final int METHODID_ROLLBACK = 15;
  private static final int METHODID_WRITE_MUTATIONS = 16;
  private static final int METHODID_CONTINUE_STREAMING = 17;
  private static final int METHODID_CONNECTION_STREAM = 18;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_INFO:
          serviceImpl.info((com.google.cloud.spannerlib.v1.InfoRequest) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.InfoResponse>) responseObserver);
          break;
        case METHODID_CREATE_POOL:
          serviceImpl.createPool((com.google.cloud.spannerlib.v1.CreatePoolRequest) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Pool>) responseObserver);
          break;
        case METHODID_CLOSE_POOL:
          serviceImpl.closePool((com.google.cloud.spannerlib.v1.Pool) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CREATE_CONNECTION:
          serviceImpl.createConnection((com.google.cloud.spannerlib.v1.CreateConnectionRequest) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Connection>) responseObserver);
          break;
        case METHODID_CLOSE_CONNECTION:
          serviceImpl.closeConnection((com.google.cloud.spannerlib.v1.Connection) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_EXECUTE:
          serviceImpl.execute((com.google.cloud.spannerlib.v1.ExecuteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.Rows>) responseObserver);
          break;
        case METHODID_EXECUTE_STREAMING:
          serviceImpl.executeStreaming((com.google.cloud.spannerlib.v1.ExecuteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData>) responseObserver);
          break;
        case METHODID_EXECUTE_BATCH:
          serviceImpl.executeBatch((com.google.cloud.spannerlib.v1.ExecuteBatchRequest) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.ExecuteBatchDmlResponse>) responseObserver);
          break;
        case METHODID_METADATA:
          serviceImpl.metadata((com.google.cloud.spannerlib.v1.Rows) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata>) responseObserver);
          break;
        case METHODID_NEXT:
          serviceImpl.next((com.google.cloud.spannerlib.v1.NextRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.ListValue>) responseObserver);
          break;
        case METHODID_RESULT_SET_STATS:
          serviceImpl.resultSetStats((com.google.cloud.spannerlib.v1.Rows) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetStats>) responseObserver);
          break;
        case METHODID_NEXT_RESULT_SET:
          serviceImpl.nextResultSet((com.google.cloud.spannerlib.v1.Rows) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.ResultSetMetadata>) responseObserver);
          break;
        case METHODID_CLOSE_ROWS:
          serviceImpl.closeRows((com.google.cloud.spannerlib.v1.Rows) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_BEGIN_TRANSACTION:
          serviceImpl.beginTransaction((com.google.cloud.spannerlib.v1.BeginTransactionRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((com.google.cloud.spannerlib.v1.Connection) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK:
          serviceImpl.rollback((com.google.cloud.spannerlib.v1.Connection) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WRITE_MUTATIONS:
          serviceImpl.writeMutations((com.google.cloud.spannerlib.v1.WriteMutationsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.spanner.v1.CommitResponse>) responseObserver);
          break;
        case METHODID_CONTINUE_STREAMING:
          serviceImpl.continueStreaming((com.google.cloud.spannerlib.v1.Rows) request,
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.RowData>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECTION_STREAM:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.connectionStream(
              (io.grpc.stub.StreamObserver<com.google.cloud.spannerlib.v1.ConnectionStreamResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.InfoRequest,
              com.google.cloud.spannerlib.v1.InfoResponse>(
                service, METHODID_INFO)))
        .addMethod(
          getCreatePoolMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.CreatePoolRequest,
              com.google.cloud.spannerlib.v1.Pool>(
                service, METHODID_CREATE_POOL)))
        .addMethod(
          getClosePoolMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Pool,
              com.google.protobuf.Empty>(
                service, METHODID_CLOSE_POOL)))
        .addMethod(
          getCreateConnectionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.CreateConnectionRequest,
              com.google.cloud.spannerlib.v1.Connection>(
                service, METHODID_CREATE_CONNECTION)))
        .addMethod(
          getCloseConnectionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Connection,
              com.google.protobuf.Empty>(
                service, METHODID_CLOSE_CONNECTION)))
        .addMethod(
          getExecuteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.ExecuteRequest,
              com.google.cloud.spannerlib.v1.Rows>(
                service, METHODID_EXECUTE)))
        .addMethod(
          getExecuteStreamingMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.ExecuteRequest,
              com.google.cloud.spannerlib.v1.RowData>(
                service, METHODID_EXECUTE_STREAMING)))
        .addMethod(
          getExecuteBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.ExecuteBatchRequest,
              com.google.spanner.v1.ExecuteBatchDmlResponse>(
                service, METHODID_EXECUTE_BATCH)))
        .addMethod(
          getMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Rows,
              com.google.spanner.v1.ResultSetMetadata>(
                service, METHODID_METADATA)))
        .addMethod(
          getNextMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.NextRequest,
              com.google.protobuf.ListValue>(
                service, METHODID_NEXT)))
        .addMethod(
          getResultSetStatsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Rows,
              com.google.spanner.v1.ResultSetStats>(
                service, METHODID_RESULT_SET_STATS)))
        .addMethod(
          getNextResultSetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Rows,
              com.google.spanner.v1.ResultSetMetadata>(
                service, METHODID_NEXT_RESULT_SET)))
        .addMethod(
          getCloseRowsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Rows,
              com.google.protobuf.Empty>(
                service, METHODID_CLOSE_ROWS)))
        .addMethod(
          getBeginTransactionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.BeginTransactionRequest,
              com.google.protobuf.Empty>(
                service, METHODID_BEGIN_TRANSACTION)))
        .addMethod(
          getCommitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Connection,
              com.google.spanner.v1.CommitResponse>(
                service, METHODID_COMMIT)))
        .addMethod(
          getRollbackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Connection,
              com.google.protobuf.Empty>(
                service, METHODID_ROLLBACK)))
        .addMethod(
          getWriteMutationsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.WriteMutationsRequest,
              com.google.spanner.v1.CommitResponse>(
                service, METHODID_WRITE_MUTATIONS)))
        .addMethod(
          getConnectionStreamMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.ConnectionStreamRequest,
              com.google.cloud.spannerlib.v1.ConnectionStreamResponse>(
                service, METHODID_CONNECTION_STREAM)))
        .addMethod(
          getContinueStreamingMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.google.cloud.spannerlib.v1.Rows,
              com.google.cloud.spannerlib.v1.RowData>(
                service, METHODID_CONTINUE_STREAMING)))
        .build();
  }

  private static abstract class SpannerLibBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SpannerLibBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.google.cloud.spannerlib.v1.SpannerLibProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SpannerLib");
    }
  }

  private static final class SpannerLibFileDescriptorSupplier
      extends SpannerLibBaseDescriptorSupplier {
    SpannerLibFileDescriptorSupplier() {}
  }

  private static final class SpannerLibMethodDescriptorSupplier
      extends SpannerLibBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    SpannerLibMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (SpannerLibGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SpannerLibFileDescriptorSupplier())
              .addMethod(getInfoMethod())
              .addMethod(getCreatePoolMethod())
              .addMethod(getClosePoolMethod())
              .addMethod(getCreateConnectionMethod())
              .addMethod(getCloseConnectionMethod())
              .addMethod(getExecuteMethod())
              .addMethod(getExecuteStreamingMethod())
              .addMethod(getExecuteBatchMethod())
              .addMethod(getMetadataMethod())
              .addMethod(getNextMethod())
              .addMethod(getResultSetStatsMethod())
              .addMethod(getNextResultSetMethod())
              .addMethod(getCloseRowsMethod())
              .addMethod(getBeginTransactionMethod())
              .addMethod(getCommitMethod())
              .addMethod(getRollbackMethod())
              .addMethod(getWriteMutationsMethod())
              .addMethod(getConnectionStreamMethod())
              .addMethod(getContinueStreamingMethod())
              .build();
        }
      }
    }
    return result;
  }
}
