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

# frozen_string_literal: true

require "spec_helper"
require "google/cloud/spanner/v1"

RSpec.describe "Connection APIs against Spanner emulator", :integration do
  before(:all) do
    @emulator_host = ENV.fetch("SPANNER_EMULATOR_HOST", nil)
    skip "SPANNER_EMULATOR_HOST not set" unless @emulator_host && !@emulator_host.empty?

    require "spannerlib/pool"
    @dsn = "projects/your-project-id/instances/test-instance/databases/test-database?autoConfigEmulator=true"

    pool = Pool.create_pool(@dsn)
    conn = pool.create_connection
    ddl_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(sql: "DROP TABLE IF EXISTS test_table"),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "CREATE TABLE test_table (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(id)"
        )
      ]
    )
    conn.execute_batch(ddl_batch_req)
    conn.close
    pool.close
  end

  before do
    @pool = Pool.create_pool(@dsn)
    @conn = @pool.create_connection
    delete_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
            table: "test_table",
            key_set: Google::Cloud::Spanner::V1::KeySet.new(all: true)
          )
        )
      ]
    )
    @conn.write_mutations(delete_req)
  end

  after do
    @conn.close
    @pool.close
  end

  it "creates a connection pool" do
    expect(@pool.id).to be > 0
    expect(@conn.conn_id).to be > 0
  end

  it "creates two connections from the same pool" do
    conn2 = @pool.create_connection
    expect(@conn.conn_id).not_to eq(conn2.conn_id)
    expect(conn2.conn_id).to be > 0
    conn2.close
  end

  it "writes and reads data in a read-write transaction" do
    @conn.begin_transaction
    insert_data_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
            table: "test_table",
            columns: %w[id name],
            values: [
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "1"),
                                                       Google::Protobuf::Value.new(string_value: "Alice")]),
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "2"),
                                                       Google::Protobuf::Value.new(string_value: "Bob")])
            ]
          )
        )
      ]
    )
    @conn.write_mutations(insert_data_req)
    @conn.commit

    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: "SELECT id, name FROM test_table ORDER BY id")
    rows_id = @conn.execute(select_req)

    all_rows = []
    loop do
      row_bytes = @conn.next_rows(rows_id, 1)
      break if row_bytes.nil? || row_bytes.empty?

      all_rows << Google::Protobuf::ListValue.decode(row_bytes)
    end
    @conn.close_rows(rows_id)

    expect(all_rows.length).to eq(2)
    expect(all_rows[0].values[1].string_value).to eq("Alice")
  end

  it "writes and reads data without an explicit transaction (autocommit)" do
    insert_data_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
            table: "test_table",
            columns: %w[id name],
            values: [
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "3"),
                                                       Google::Protobuf::Value.new(string_value: "Charlie")]),
              Google::Protobuf::ListValue.new(values: [Google::Protobuf::Value.new(string_value: "4"),
                                                       Google::Protobuf::Value.new(string_value: "David")])
            ]
          )
        )
      ]
    )
    @conn.write_mutations(insert_data_req)

    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(sql: "SELECT id, name FROM test_table ORDER BY id")
    rows_id = @conn.execute(select_req)

    all_rows = []
    loop do
      row_bytes = @conn.next_rows(rows_id, 1)
      break if row_bytes.nil? || row_bytes.empty?

      all_rows << Google::Protobuf::ListValue.decode(row_bytes)
    end
    @conn.close_rows(rows_id)

    expect(all_rows.length).to eq(2)
    expect(all_rows[0].values[1].string_value).to eq("Charlie")
  end

  it "raises an error when writing in a read-only transaction" do
    transaction_options = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_only: Google::Cloud::Spanner::V1::TransactionOptions::ReadOnly.new(strong: true)
    )
    @conn.begin_transaction(transaction_options)

    insert_data_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          insert: Google::Cloud::Spanner::V1::Mutation::Write.new(table: "test_table")
        )
      ]
    )
    expect { @conn.write_mutations(insert_data_req) }.to raise_error(RuntimeError, /read-only transactions cannot write/)

    @conn.rollback
  end
end
