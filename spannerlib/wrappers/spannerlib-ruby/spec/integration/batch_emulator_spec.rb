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

RSpec.describe "Batch API test", :integration do
  before(:all) do
    @emulator_host = ENV.fetch("SPANNER_EMULATOR_HOST", nil)
    skip "SPANNER_EMULATOR_HOST not set" unless @emulator_host && !@emulator_host.empty?

    require "spannerlib/pool"
    @dsn = "projects/your-project-id/instances/test-instance/databases/test-database?autoConfigEmulator=true"

    @pool = Pool.create_pool(@dsn)
    conn = @pool.create_connection
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
  end

  after(:all) do
    @pool&.close
  end

  before do
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
    @conn&.close
  end

  it "tests a batch DML request" do
    dml_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "INSERT INTO test_table (id, name) VALUES (1, 'name1')"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "INSERT INTO test_table (id, name) VALUES (2, 'name2')"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "UPDATE test_table SET name='name3' WHERE id=1"
        )
      ]
    )
    resp = @conn.execute_batch(dml_batch_req)
    expect(resp.result_sets.length).to eq 3

    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(
      sql: "SELECT id, name FROM test_table ORDER BY id"
    )
    rows = @conn.execute(select_req)
    all_rows = rows.map { |row_bytes| Google::Protobuf::ListValue.decode(row_bytes) }

    expect(all_rows.length).to eq 2
    expect(all_rows[0].values[0].string_value).to eq "1"
    expect(all_rows[0].values[1].string_value).to eq "name3"
    expect(all_rows[1].values[0].string_value).to eq "2"
    expect(all_rows[1].values[1].string_value).to eq "name2"
  end

  it "tests a batch DDL request" do
    ddl_batch_req = Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest.new(
      statements: [
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "DROP TABLE IF EXISTS test_table"
        ),
        Google::Cloud::Spanner::V1::ExecuteBatchDmlRequest::Statement.new(
          sql: "CREATE TABLE test_table (key INT64 NOT NULL, data STRING(MAX)) PRIMARY KEY(key)"
        )
      ]
    )

    expect { @conn.execute_batch(ddl_batch_req) }.not_to raise_error

    insert_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
            table: "test_table",
            columns: %w[key data],
            values: [
              Google::Protobuf::ListValue.new(values: [
                                                Google::Protobuf::Value.new(string_value: "101"),
                                                Google::Protobuf::Value.new(string_value: "VerificationData")
                                              ])
            ]
          )
        )
      ]
    )
    expect { @conn.write_mutations(insert_req) }.not_to raise_error
  end

  it "queries data using parameters" do
    insert_req = Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(
      mutations: [
        Google::Cloud::Spanner::V1::Mutation.new(
          insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
            table: "test_table",
            columns: %w[key data],
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
    @conn.write_mutations(insert_req)

    # Execute the parameterized query.
    select_req = Google::Cloud::Spanner::V1::ExecuteSqlRequest.new(
      sql: "SELECT key, data FROM test_table WHERE data = @dataParam",
      params: Google::Protobuf::Struct.new(
        fields: {
          "dataParam" => Google::Protobuf::Value.new(string_value: "Alice")
        }
      ),
      param_types: {
        "dataParam" => Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::STRING)
      }
    )
    rows = @conn.execute(select_req)
    all_rows = rows.map { |row_bytes| Google::Protobuf::ListValue.decode(row_bytes) }

    expect(all_rows.length).to eq(1)
    expect(all_rows[0].values[1].string_value).to eq("Alice")
  end
end
