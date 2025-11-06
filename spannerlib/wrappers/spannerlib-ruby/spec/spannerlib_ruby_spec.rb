# frozen_string_literal: true

# rubocop:disable RSpec/NoExpectationExample

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

require "minitest/autorun"

require "grpc"
require "google/rpc/error_details_pb"
require "google/spanner/v1/spanner_pb"
require "google/spanner/v1/spanner_services_pb"
require "google/cloud/spanner/v1/spanner"

require_relative "mock_server/statement_result"
require_relative "mock_server/spanner_mock_server"

require_relative "../lib/spannerlib/ffi"
require_relative "../lib/spannerlib/connection"
require_relative "../lib/spannerlib/rows"

READ_PIPE, WRITE_PIPE = IO.pipe
GRPC.prefork

SERVER_PID = fork do
  GRPC.postfork_child
  READ_PIPE.close
  $stdout.sync = true
  $stderr.sync = true

  begin
    server = GRPC::RpcServer.new
    port = server.add_http2_port "localhost:0", :this_port_is_insecure
    mock = SpannerMockServer.new
    server.handle mock

    WRITE_PIPE.puts port
    WRITE_PIPE.close

    server.run
  rescue StandardError => e
    warn "Mock server failed to start: #{e.message}"
    exit(1)
  end
end

GRPC.postfork_parent
WRITE_PIPE.close

SERVER_PORT = READ_PIPE.gets.strip.to_i
READ_PIPE.close

describe "Connection" do
  before do
    server_address = "localhost:#{SERVER_PORT}"
    database_path = "projects/p/instances/i/databases/d"

    @dsn = "#{server_address}/#{database_path}?useplaintext=true"

    @pool_id = SpannerLib.create_pool(@dsn)
    @conn_id = SpannerLib.create_connection(@pool_id)
    @conn = Connection.new(@pool_id, @conn_id)
  end

  after do
    @conn&.close
    SpannerLib.close_pool(@pool_id) if @pool_id
  end

  it "can execute SELECT 1" do
    request_proto = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(
      sql: "SELECT 1"
    )
    rows_object = @conn.execute(request_proto)

    decoded_rows = rows_object.map do |row_bytes|
      Google::Protobuf::ListValue.decode(row_bytes)
    end

    _(decoded_rows.length).must_equal 1
    _(decoded_rows[0].values[0].string_value).must_equal "1"
  end

  it "can execute a read-write transaction (Begin/Insert/Commit)" do
    tx_opts = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new
    )
    @conn.begin_transaction(tx_opts)

    sql = "INSERT INTO test_table (id, name) VALUES ('1', 'Alice')"
    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)

    @conn.execute(req)
    commit_resp = @conn.commit

    _(commit_resp.commit_timestamp).wont_be_nil
  end
end

# rubocop:enable RSpec/NoExpectationExample

# --- 5. GLOBAL SHUTDOWN HOOK ---
Minitest.after_run do
  if SERVER_PID
    Process.kill("KILL", SERVER_PID)
    Process.wait(SERVER_PID)
  end
end
