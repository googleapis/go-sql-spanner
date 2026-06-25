const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Define paths to the proto and the googleapis dependency folder
// Note: Adjusted to point from 'ipc/src/grpc/bindings.js' back to 'grpc-server/'
const PROTO_ROOT = path.resolve(__dirname, '../../../../../grpc-server');
const PROTO_PATH = path.join(PROTO_ROOT, 'google/spannerlib/v1/spannerlib.proto');
const GOOGLEAPIS_PATH = path.join(PROTO_ROOT, 'googleapis');

// Load the proto file at runtime
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [GOOGLEAPIS_PATH, PROTO_ROOT] // Resolves all 'google/api/...' imports
});

// Load the package definition into gRPC
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);

// The SpannerLib service definition is now available through the descriptor
const SpannerLib = protoDescriptor.google.spannerlib.v1.SpannerLib;

const client = new SpannerLib('localhost:50051', grpc.credentials.createInsecure());

// We export the v1 package which contains all message constructors (CommitRequest, etc.)
const messages = protoDescriptor.google.spannerlib.v1;

module.exports = {
    client,
    messages
};
