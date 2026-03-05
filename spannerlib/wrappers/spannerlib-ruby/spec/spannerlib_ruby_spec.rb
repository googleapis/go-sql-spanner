# frozen_string_literal: true

# rubocop:disable Style/GlobalVars
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
require "google/cloud/spanner/v1/spanner"
require "timeout"
require "tmpdir"

require_relative "mock_server/statement_result"
require_relative "../lib/spannerlib/ffi"
require_relative "../lib/spannerlib/connection"

$server_pid = nil
$server_port = nil
$port_file = nil
$any_failure = false
$mock_msg_file = nil
$mock_req_file = nil

Minitest.after_run do
  if $server_pid
    begin
      if Gem.win_platform?
        Process.kill("KILL", $server_pid)
      else
        Process.kill("TERM", $server_pid)
      end
      Process.wait($server_pid)
    rescue Errno::ESRCH, Errno::ECHILD, Errno::EINVAL
      # Process already dead
    end
  end
  [$port_file, $mock_msg_file, $mock_req_file].each do |f|
    File.delete(f) if f && File.exist?(f)
  end

  if $any_failure
    puts "Tests Failed! Exiting with code 1" # rubocop:disable RSpec/Output
    $stdout.flush
    Process.exit!(1)
  else
    puts "Tests Passed! Exiting with code 0" # rubocop:disable RSpec/Output
    $stdout.flush
    Process.exit!(0)
  end
end

describe "Connection" do
  def self.spawn_server
    project_root = File.expand_path("..", __dir__)
    runner_path = File.join(project_root, "spec", "mock_server_runner.rb")
    ruby_exe = Gem.ruby

    if Gem.win_platform?
      runner_path = runner_path.tr("/", "\\")
      ruby_exe = ruby_exe.tr("/", "\\")
    end

    $port_file = File.join(Dir.tmpdir, "spanner_mock_port_#{Time.now.to_i}_#{rand(1000)}.txt")
    $mock_msg_file = File.join(Dir.tmpdir, "spanner_mock_msgs_#{Process.pid}_#{SecureRandom.hex(4)}.bin")
    $mock_req_file = File.join(Dir.tmpdir, "spanner_mock_reqs_#{Process.pid}_#{SecureRandom.hex(4)}.bin")

    env_vars = {
      "MOCK_PORT_FILE" => $port_file,
      "MOCK_MSG_FILE" => $mock_msg_file,
      "MOCK_REQ_FILE" => $mock_req_file
    }
    cmd = [ruby_exe, "-Ilib", "-Ispec", runner_path]
    $stdout.flush

    spawn(env_vars, *cmd, out: $stdout, err: $stderr)
  end

  def self.wait_for_port
    start_time = Time.now
    loop do
      raise "Timed out waiting for mock server port file." if Time.now - start_time > 20

      if File.exist?($port_file)
        content = File.read($port_file).strip
        return content.to_i unless content.empty?
      end

      raise "Mock server exited unexpectedly!" if Process.waitpid($server_pid, Process::WNOHANG)

      sleep 0.1
    end
  end

  def self.ensure_server_running!
    return if $server_port

    $server_pid = spawn_server

    begin
      $server_port = wait_for_port
    rescue StandardError
      Process.kill("TERM", $server_pid) if $server_pid
      raise
    end
  end

  # Helper to set the mock server's response for a given SQL statement
  def set_mock_result(sql, result)
    existing = {}
    if File.exist?($mock_msg_file)
      content = File.binread($mock_msg_file)
      begin
        existing = Marshal.load(content) unless content.empty?
      rescue ArgumentError, TypeError => e
        warn "Failed to load existing mocks: #{e.message}"
        existing = {}
      end
    end
    existing[sql] = result
    File.binwrite($mock_msg_file, Marshal.dump(existing))
  end

  # Helper to get the requests received by the mock server
  def load_requests
    return [] unless File.exist?($mock_req_file)

    requests = []
    File.open($mock_req_file, "rb") do |f|
      until f.eof?
        begin
          data = Marshal.load(f)
          klass = Object.const_get(data[:class])
          requests << klass.decode(data[:payload])
        rescue ArgumentError, TypeError, NameError => e
          warn "Failed to load a request from log: #{e.message}"
          break
        end
      end
    end
    requests
  end

  def find_transaction_options(requests)
    # Look for explicit BeginTransactionRequest
    begin_req = requests.find { |r| r.is_a?(Google::Cloud::Spanner::V1::BeginTransactionRequest) }
    return begin_req.options if begin_req

    # Look for inline TransactionOptions in ExecuteSqlRequest
    exec_req = requests.find do |r|
      r.is_a?(Google::Cloud::Spanner::V1::ExecuteSqlRequest) &&
        r.transaction&.begin
    end
    return exec_req.transaction.begin if exec_req

    nil
  end

  before do
    self.class.ensure_server_running!
    File.binwrite($mock_msg_file, Marshal.dump({}))
    File.write($mock_req_file, "")
    @dsn = "127.0.0.1:#{$server_port}/projects/p/instances/i/databases/d?useplaintext=true"

    @pool_id = SpannerLib.create_pool(@dsn)
    @conn_id = SpannerLib.create_connection(@pool_id)
    @conn = Connection.new(@pool_id, @conn_id)
  end

  after do
    @conn&.close
    SpannerLib.close_pool(@pool_id) if @pool_id
    $any_failure = true unless failures.empty?
  end

  it "can execute SELECT 1" do
    set_mock_result("SELECT 1", StatementResult.create_select1_result)

    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: "SELECT 1")
    rows = @conn.execute(req)

    decoded = rows.map { |r| Google::Protobuf::ListValue.decode(r) }
    _(decoded.length).must_equal 1
    _(decoded[0].values[0].string_value).must_equal "1"
  end

  it "validates read-write transaction lifecycle" do
    set_mock_result(
      "INSERT INTO test_table (id, name) VALUES ('1', 'Alice')",
      StatementResult.create_update_count_result(1)
    )

    tx_opts = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new
    )
    @conn.begin_transaction(tx_opts)

    sql = "INSERT INTO test_table (id, name) VALUES ('1', 'Alice')"
    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    @conn.execute(req)
    @conn.commit

    requests = load_requests

    tx_options = find_transaction_options(requests)
    _(tx_options).wont_be_nil
    _(tx_options.read_write).wont_be_nil

    commit_req = requests.find { |r| r.is_a?(Google::Cloud::Spanner::V1::CommitRequest) }
    _(commit_req).wont_be_nil
    _(commit_req.transaction_id).wont_be_empty
  end

  it "validates read-only transaction lifecycle" do
    set_mock_result(
      "SELECT 1",
      StatementResult.create_select1_result
    )

    tx_opts = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_only: Google::Cloud::Spanner::V1::TransactionOptions::ReadOnly.new(strong: true)
    )
    @conn.begin_transaction(tx_opts)

    sql = "SELECT 1"
    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    @conn.execute(req)
    @conn.commit

    requests = load_requests

    tx_options = find_transaction_options(requests)
    _(tx_options).wont_be_nil
    _(tx_options.read_only).wont_be_nil
    _(tx_options.read_only.strong).must_equal true

    exec_req = requests.find { |r| r.is_a?(Google::Cloud::Spanner::V1::ExecuteSqlRequest) }
    _(exec_req).wont_be_nil

    commit_reqs = requests.select { |r| r.is_a?(Google::Cloud::Spanner::V1::CommitRequest) }
    _(commit_reqs).must_be_empty "Expected no Commit requests for read-only transaction"

    rollback_reqs = requests.select { |r| r.is_a?(Google::Cloud::Spanner::V1::RollbackRequest) }
    _(rollback_reqs).must_be_empty "Expected no Rollback requests for read-only transaction"
  end

  it "raises an error when the table does not exist" do
    sql = "SELECT * FROM non_existent_table"

    error_status = Google::Rpc::Status.new(
      code: GRPC::Core::StatusCodes::NOT_FOUND,
      message: "Table not found: non_existent_table"
    )

    mock_result = StatementResult.create_exception_result(error_status)
    set_mock_result(sql, mock_result)

    err = _ do
      req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
      @conn.execute(req)
    end.must_raise StandardError

    _(err.message).must_match(/Table not found/)
  end

  it "can write mutations" do
    mutation = Google::Cloud::Spanner::V1::Mutation.new(
      insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
        table: "users",
        columns: %w[id name],
        values: [
          Google::Protobuf::ListValue.new(values: [
                                            Google::Protobuf::Value.new(string_value: "1"),
                                            Google::Protobuf::Value.new(string_value: "Alice")
                                          ])
        ]
      )
    )
    mutation_group = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [mutation]
    )
    @conn.write_mutations(mutation_group)

    requests = load_requests
    commit_req = requests.find { |r| r.is_a?(Google::Cloud::Spanner::V1::CommitRequest) }

    _(commit_req).wont_be_nil
    _(commit_req.mutations.length).must_equal 1
    _(commit_req.mutations[0].insert.table).must_equal "users"
  end

  it "can execute a batch of DML statements" do
    sql1 = "UPDATE users SET active = true WHERE id = 1"
    sql2 = "UPDATE users SET active = true WHERE id = 2"

    set_mock_result(sql1, StatementResult.create_update_count_result(1))
    set_mock_result(sql2, StatementResult.create_update_count_result(1))

    batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(sql: sql1),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(sql: sql2)
      ]
    )

    resp = @conn.execute_batch(batch_req)

    _(resp.result_sets.length).must_equal 2
    _(resp.result_sets[0].stats.row_count_exact).must_equal 1
    _(resp.result_sets[1].stats.row_count_exact).must_equal 1

    requests = load_requests
    captured_batch = requests.find { |r| r.is_a?(Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest) }

    _(captured_batch).wont_be_nil
    _(captured_batch.statements.length).must_equal 2
    _(captured_batch.statements[0].sql).must_equal sql1
  end

  it "handles Batch DML errors correctly" do
    sql_success = "UPDATE users SET active = true WHERE id = 1"
    sql_fail = "UPDATE users SET active = true WHERE id = 999"

    set_mock_result(sql_success, StatementResult.create_update_count_result(1))

    error_status = Google::Rpc::Status.new(
      code: GRPC::Core::StatusCodes::PERMISSION_DENIED,
      message: "Permission denied for user"
    )
    set_mock_result(sql_fail, StatementResult.create_exception_result(error_status))

    batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(sql: sql_success),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(sql: sql_fail)
      ]
    )

    err = _ { @conn.execute_batch(batch_req) }.must_raise StandardError
    _(err.message).must_match(/Permission denied/)
  end

  it "can execute a multi-statement query with multiple result sets" do
    sql = "SELECT 1; SELECT 2"
    set_mock_result("SELECT 1", StatementResult.create_select1_result)

    set_mock_result(" SELECT 2", StatementResult.create_single_int_result_set("Col1", 2))

    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    rows = @conn.execute(req)

    row1 = rows.next
    _(row1).wont_be_nil
    decoded1 = Google::Protobuf::ListValue.decode(row1)
    _(decoded1.values[0].string_value).must_equal "1"

    _(rows.next).must_be_nil

    metadata = rows.next_result_set
    _(metadata).wont_be_nil
    _(metadata.row_type.fields[0].name).must_equal "Col1"

    row2 = rows.next
    _(row2).wont_be_nil
    decoded2 = Google::Protobuf::ListValue.decode(row2)
    _(decoded2.values[0].string_value).must_equal "2"

    _(rows.next).must_be_nil

    _(rows.next_result_set).must_be_nil
  end

  it "returns cached metadata and stats after closing rows" do
    sql = "UPDATE users SET active = true WHERE id = 1"
    set_mock_result(sql, StatementResult.create_update_count_result(5))

    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    rows = @conn.execute(req)

    while rows.next; end

    stats_before = rows.result_set_stats
    _(stats_before).wont_be_nil
    _(stats_before.row_count_exact).must_equal 5

    metadata_before = rows.metadata
    _(metadata_before).wont_be_nil

    rows.close

    # Metadata should be cached and available even after close
    metadata_after = rows.metadata
    _(metadata_after).wont_be_nil
    _(metadata_after).must_equal metadata_before

    # Result set stats should be cached and available even after close
    stats_after = rows.result_set_stats
    _(stats_after).wont_be_nil
    _(stats_after.row_count_exact).must_equal 5

    _(rows.next).must_be_nil

    _(rows.next_result_set).must_be_nil
  end

  it "returns nil for metadata if closed before fetching" do
    sql = "SELECT 1"
    set_mock_result(sql, StatementResult.create_select1_result)

    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    rows = @conn.execute(req)

    rows.close

    # 3. Verify metadata is nil (because it was never cached, and we can't fetch it now)
    _(rows.metadata).must_be_nil
    _(rows.result_set_stats).must_be_nil
  end

  it "clears and updates stats when moving to next result set (Multi-DML)" do
    sql = "UPDATE A SET x=1; UPDATE B SET x=2"

    set_mock_result("UPDATE A SET x=1", StatementResult.create_update_count_result(10))
    set_mock_result(" UPDATE B SET x=2", StatementResult.create_update_count_result(20))

    req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: sql)
    rows = @conn.execute(req)

    while rows.next; end

    stats1 = rows.result_set_stats
    _(stats1).wont_be_nil
    _(stats1.row_count_exact).must_equal 10

    # calling next_result_set should clear the internal @stats cache
    metadata = rows.next_result_set
    _(metadata).wont_be_nil

    while rows.next; end

    stats2 = rows.result_set_stats
    _(stats2).wont_be_nil

    # Verify stats have been updated and earlier cache was cleared
    _(stats2.row_count_exact).must_equal 20
    _(stats2.row_count_exact).wont_equal stats1.row_count_exact

    _(rows.next_result_set).must_be_nil
  end
end
# rubocop:enable RSpec/NoExpectationExample
# rubocop:enable Style/GlobalVars
