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
    puts "Tests Failed! Exiting with code 1"
    $stdout.flush
    Process.exit!(1)
  else
    puts "Tests Passed! Exiting with code 0"
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
    $mock_msg_file = File.join(Dir.tmpdir, "spanner_mock_msgs_#{Time.now.to_i}_#{rand(1000)}.bin")
    $mock_req_file = File.join(Dir.tmpdir, "spanner_mock_reqs_#{Time.now.to_i}_#{rand(1000)}.bin")

    env_vars = {
      "MOCK_PORT_FILE" => $port_file,
      "MOCK_MSG_FILE" => $mock_msg_file,
      "MOCK_REQ_FILE" => $mock_req_file
    }
    cmd = [ruby_exe, "-Ilib", "-Ispec", runner_path]

    puts "DEBUG: Spawning: #{cmd}"
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
        {}
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

    # Verify Transaction Options (Explicit or Inline)
    # Often client libraries send the options inline with ExecuteSql
    # use the helper find_transaction_options to verfy both the cases (inline or explicit BeginTransaction)
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

    # Verify Transaction Options (Explicit or Inline)
    # Often client libraries send the options inline with ExecuteSql
    # use the helper find_transaction_options to verfy both the cases (inline or explicit BeginTransaction)
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
end
# rubocop:enable RSpec/NoExpectationExample
# rubocop:enable Style/GlobalVars
