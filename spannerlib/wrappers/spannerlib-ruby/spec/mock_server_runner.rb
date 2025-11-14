# frozen_string_literal: true

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$stdout.sync = true

require "grpc"
require "google/cloud/spanner/v1/spanner"
require "google/spanner/v1/spanner_pb"
require "google/spanner/v1/spanner_services_pb"

require_relative "mock_server/spanner_mock_server"

Signal.trap("TERM") do
  exit!(0) # "exit skips cleanup hooks and prevents gRPC segfaults
end

begin
  server = GRPC::RpcServer.new
  port = server.add_http2_port "127.0.0.1:0", :this_port_is_insecure
  server.handle SpannerMockServer.new
  File.write(ENV["MOCK_PORT_FILE"], port.to_s) if ENV["MOCK_PORT_FILE"]

  # 2. Print ONLY the port number to stdout for the parent to read
  puts port
  server.run_till_terminated
rescue SignalException
  exit(0)
rescue StandardError => e
  warn "Mock server crashed: #{e.message}"
  warn e.backtrace.join("\n")
  exit(1)
end
