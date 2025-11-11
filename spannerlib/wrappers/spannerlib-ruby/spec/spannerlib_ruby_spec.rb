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
require "tmpdir" # Needed for the file-based handshake

require_relative "../lib/spannerlib/ffi"
require_relative "../lib/spannerlib/connection"

$server_pid = nil
$server_port = nil
$port_file = nil
$any_failure = false

Minitest.after_run do
  if $server_pid
    begin
      Process.kill("TERM", $server_pid)
      Process.wait($server_pid)
    rescue Errno::ESRCH, Errno::ECHILD, Errno::EINVAL
      # Process already dead
    end
  end
  File.delete($port_file) if $port_file && File.exist?($port_file)
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
    runner_path = File.expand_path(File.join(__dir__, "mock_server_runner.rb"))

    $port_file = File.join(Dir.tmpdir, "spanner_mock_port_#{Time.now.to_i}_#{rand(1000)}.txt")

    # 2. Tell the runner where to write the port
    env_vars = { "MOCK_PORT_FILE" => $port_file }

    # 3. Spawn process inheriting stdout/stderr.
    # This prevents the buffer deadlock.
    Process.spawn(env_vars, RbConfig.ruby, "-Ilib", "-Ispec", runner_path, out: $stdout, err: $stderr)
  end

  def self.wait_for_port
    start_time = Time.now
    loop do
      raise "Timed out waiting for mock server to write to #{$port_file}" if Time.now - start_time > 20

      # 4. Poll the file for the port
      if File.exist?($port_file)
        content = File.read($port_file).strip
        return content.to_i unless content.empty?
      end

      # Check if the server died
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

  before do
    self.class.ensure_server_running!
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
    request_proto = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: "SELECT 1")
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
# rubocop:enable Style/GlobalVars
